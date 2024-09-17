const neo4j = require('neo4j-driver');
const { TextSplitter } = require("../../TextSplitter");
const { SystemSettings } = require("../../../models/systemSettings");
const { v4: uuidv4 } = require("uuid");
const { storeVectorResult, cachedVectorInformation } = require("../../files");
const { toChunks, getEmbeddingEngineSelection } = require("../../helpers");
const { sourceIdentifier } = require("../../chats");

const debugLog = (message, data = null) => {
  if (process.env.DEBUG_NEO4J === 'true') {
    console.log(`[Neo4j Debug] ${message}`, data ? JSON.stringify(data, null, 2) : '');
  }
};

const log = (level, message, ...args) => {
  console[level](`Neo4j::${message}`, ...args);
};

const handleError = (error, customMessage) => {
  log('error', `${customMessage} - ${error.message}`);
  debugLog('Error', { customMessage, errorMessage: error.message });
  return { error: error.message };
};

const Neo4jDB = {
  name: 'Neo4j',
  driver: null,

  initialize: async function() {
    if (process.env.VECTOR_DB !== 'neo4j') {
      throw new Error('Neo4j::Invalid ENV settings');
    }

    log('log', 'Attempting connection with:', {
      uri: process.env.NEO4J_URI,
      user: process.env.NEO4J_USER,
    });

    this.driver = neo4j.driver(
      process.env.NEO4J_URI,
      neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
    );

    try {
      await this.driver.verifyConnectivity();
      log('log', 'Connection established');
      await this.updateGraphAndRelationships();
    } catch (error) {
      throw handleError(error, 'Connection failed');
    }
  },

  createGraphProjection: async function() {
    const session = await this.getSession();
    try {
      // Prüfen, ob der Graph bereits existiert
      const existsResult = await session.run(`
        CALL gds.graph.exists('chunkGraph')
        YIELD exists
      `);
      const graphExists = existsResult.records[0].get('exists');

      if (graphExists) {
        // Versuchen, den Graphen zu aktualisieren
        try {
          await session.run(`
            CALL gds.graph.project.cypher(
              'chunkGraph',
              'MATCH (c:Chunk) RETURN id(c) AS id, c.embedding AS embedding',
              'MATCH (c1:Chunk)-[r:SIMILAR_TO]->(c2:Chunk) RETURN id(c1) AS source, id(c2) AS target, r.similarity AS weight',
              {
                nodeProperties: ['embedding'],
                relationshipProperties: ['similarity']
              }
            )
          `);
          console.log("Graph projection updated successfully.");
        } catch (updateError) {
          // Wenn das Update fehlschlägt, löschen wir den Graphen und erstellen ihn neu
          console.log("Failed to update graph. Dropping and recreating...");
          await session.run(`
            CALL gds.graph.drop('chunkGraph')
            YIELD graphName
          `);
          await this.createNewGraphProjection(session);
        }
      } else {
        // Wenn der Graph nicht existiert, erstellen wir ihn neu
        await this.createNewGraphProjection(session);
      }
    } catch (error) {
      console.error("Error in graph projection process:", error);
    } finally {
      await session.close();
    }
  },

  createNewGraphProjection: async function(session) {
    await session.run(`
      CALL gds.graph.project(
        'chunkGraph',
        'Chunk',
        '*',
        {
          nodeProperties: ['embedding']
        }
      )
    `);
    console.log("New graph projection created successfully.");
  },

  createKNNRelationships: async function(k = 5) {
    const session = await this.getSession();
    try {
      // Zuerst alle bestehenden SIMILAR_TO Beziehungen löschen
      await session.run(`
        MATCH ()-[r:SIMILAR_TO]->()
        DELETE r
      `);
      console.log("Existing KNN relationships deleted.");

      // Dann neue KNN-Beziehungen erstellen
      await session.run(`
        CALL gds.knn.write('chunkGraph', {
          topK: $k,
          nodeProperties: ['embedding'],
          writeRelationshipType: 'SIMILAR_TO',
          writeProperty: 'similarity',
          concurrency: 4
        })
      `, { k: neo4j.int(k) });
      console.log("New KNN relationships created successfully.");
    } catch (error) {
      console.error("Error creating KNN relationships:", error);
    } finally {
      await session.close();
    }
  },

  updateGraphAndRelationships: async function() {
    await this.createOrUpdateVectorIndex();
    await this.createGraphProjection();
    await this.createKNNRelationships();
  },

  getEmbeddingDimensions: async function() {
    const session = await this.getSession();
    try {
      const result = await session.run(`
        MATCH (c:Chunk)
        WHERE c.embedding IS NOT NULL
        WITH size(c.embedding) AS embeddingDim
        LIMIT 1
        RETURN embeddingDim
      `);
      return result.records[0]?.get('embeddingDim') || null;
    } finally {
      await session.close();
    }
  },

  createOrUpdateVectorIndex: async function() {
    const session = await this.getSession();
    try {
      const embeddingDim = await this.getEmbeddingDimensions();
      if (!embeddingDim) {
        console.log("No embeddings found. Skipping vector index creation.");
        return;
      }

      // Versuchen, den Index zu erstellen
      try {
        await session.run(`
          CALL db.index.vector.createNodeIndex(
            'chunkEmbeddingIndex',
            'Chunk',
            'embedding',
            $embeddingDim,
            'cosine'
          )
        `, { embeddingDim });
        console.log("Vector index created successfully.");
      } catch (indexError) {
        // Wenn der Index bereits existiert, ist das kein Problem
        if (indexError.message.includes("An equivalent index already exists")) {
          console.log("Vector index already exists. No update needed as it updates automatically.");
        } else {
          // Wenn es ein anderer Fehler ist, werfen wir ihn weiter
          throw indexError;
        }
      }
    } catch (error) {
      console.error("Error creating vector index:", error);
    } finally {
      await session.close();
    }
  },

  updateGraphProjectionAndKNN: async function () {
    const session = await this.getSession();
    try {
      // Schritt 0: Prüfen, ob der Graph existiert und ihn gegebenenfalls löschen
      const graphExistsResult = await session.run(`
        CALL gds.graph.exists('chunkGraph')
        YIELD exists
        RETURN exists
      `);
      const graphExists = graphExistsResult.records[0].get('exists');

      if (graphExists) {
        await session.run(`
          CALL gds.graph.drop('chunkGraph')
          YIELD graphName
        `);
        console.log("Existing graph dropped");
      }

      // Schritt 1: Graphprojektion erstellen
      await session.run(`
        CALL gds.graph.project.cypher(
          'chunkGraph',
          'MATCH (c:Chunk) RETURN id(c) AS id, c.embedding AS embedding',
          'MATCH (c1:Chunk)-[r:SIMILAR_TO]->(c2:Chunk) RETURN id(c1) AS source, id(c2) AS target, r.similarity AS weight'
        )
      `);
      console.log("Graph projection created");

      // Schritt 2: KNN-Beziehungen berechnen
      await session.run(`
        CALL gds.knn.write(
          'chunkGraph',
          {
            topK: 5,
            nodeProperties: ['embedding'],
            similarityCutoff: 0.5,
            writeRelationshipType: 'SIMILAR_TO',
            writeProperty: 'similarity'
          }
        )
      `);
      console.log("KNN relationships calculated and written");
    } catch (error) {
      console.error("Error updating graph projection and KNN:", error);
    } finally {
      await session.close();
    }
  },

  createEmbeddingIndex: async function() {
    const session = await this.getSession();
    try {
      await session.run(`
        CREATE INDEX embedding_index IF NOT EXISTS
        FOR (c:Chunk)
        ON (c.embedding)
      `);
      log('log', 'Embedding index created or already exists');
    } catch (error) {
      handleError(error, 'Failed to create embedding index');
    } finally {
      await session.close();
    }
  },

  disconnect: async function() {
    if (this.driver) {
      await this.driver.close();
      this.driver = null;
    }
  },

  getSession: async function() {
    if (!this.driver) {
      await this.initialize();
    }
    debugLog('New session created');
    return this.driver.session();
  },

  heartbeat: async function() {
    const session = await this.getSession();
    try {
      await session.run('RETURN 1');
      return { heartbeat: true };
    } catch (error) {
      return handleError(error, 'Heartbeat failed');
    } finally {
      await session.close();
    }
  },

  hasNamespace: async function(namespace) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (c:Chunk:${namespace}) RETURN count(c) as count LIMIT 1`
      );
      return result.records[0].get('count').toNumber() > 0;
    } catch (error) {
      return handleError(error, 'Failed to check namespace');
    } finally {
      await session.close();
    }
  },

  namespaceCount: async function(namespace) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (c:Chunk:${namespace}) RETURN count(c) as count`
      );
      return result.records[0].get('count').toNumber();
    } catch (error) {
      return handleError(error, 'Failed to count namespace');
    } finally {
      await session.close();
    }
  },

  addDocumentToNamespace: async function(namespace, documentData, fullFilePath = null, skipCache = false) {
    const session = await this.getSession();
    try {
      const { pageContent, docId, ...metadata } = documentData;
      if (!pageContent || pageContent.length == 0) return false;
  
      debugLog("Adding new vectorized document into namespace", { namespace, docId });
  
      if (skipCache) {
        const cacheResult = await cachedVectorInformation(fullFilePath);
        if (cacheResult.exists) {
          debugLog("Using cached vector information", { fullFilePath });
          const { chunks } = cacheResult;
          for (const chunk of chunks) {
            const result = await session.run(
              `CREATE (c:Chunk:${namespace} {
                docId: $docId,
                chunkId: $chunkId,
                pageContent: $pageContent,
                metadata: $metadata,
                embedding: $embedding
              })
              RETURN c`,
              {
                docId,
                chunkId: uuidv4(),
                pageContent: chunk.metadata.text,
                metadata: JSON.stringify(chunk.metadata),
                embedding: chunk.values
              }
            );
          }
          debugLog(`Processed ${chunks.length} cached chunks`);
          
          // Aktualisieren Sie den Graph und die Beziehungen nach dem Hinzufügen neuer Chunks
          await this.updateGraphAndRelationships();
          
          return { vectorized: true, error: null };
        }
      }
  
      const EmbedderEngine = getEmbeddingEngineSelection();
      const textSplitter = new TextSplitter({
        chunkSize: TextSplitter.determineMaxChunkSize(
          await SystemSettings.getValueOrFallback({
            label: "text_splitter_chunk_size",
          }),
          EmbedderEngine?.embeddingMaxChunkLength
        ),
        chunkOverlap: await SystemSettings.getValueOrFallback(
          { label: "text_splitter_chunk_overlap" },
          20
        ),
        chunkHeaderMeta: {
          sourceDocument: metadata?.title,
          published: metadata?.published || "unknown",
        },
      });
      const textChunks = await textSplitter.splitText(pageContent);
  
      debugLog("Chunks created from document", { count: textChunks.length });
      const vectors = [];
      const vectorValues = await EmbedderEngine.embedChunks(textChunks);
  
      if (vectorValues && vectorValues.length > 0) {
        debugLog(`Processing ${vectorValues.length} chunks`);
        for (const [i, vector] of vectorValues.entries()) {
          const chunkId = uuidv4();
          const chunkMetadata = { ...metadata, text: textChunks[i] };
          
          if (i === 0 || i === vectorValues.length - 1) {
            debugLog(`Processing chunk ${i + 1}/${vectorValues.length}`, { namespace, docId, chunkId });
          }
  
          const result = await session.run(
            `CREATE (c:Chunk:${namespace} {
              docId: $docId,
              chunkId: $chunkId,
              pageContent: $pageContent,
              metadata: $metadata,
              embedding: $embedding
            })
            RETURN c`,
            {
              docId,
              chunkId,
              pageContent: textChunks[i],
              metadata: JSON.stringify(chunkMetadata),
              embedding: vector
            }
          );
  
          if (i === 0 || i === vectorValues.length - 1) {
            debugLog(`Chunk ${i + 1} created`);
          }
  
          vectors.push({ id: chunkId, values: vector, metadata: chunkMetadata });
        }
        debugLog(`All ${vectorValues.length} chunks processed and added to the database`);
        await storeVectorResult([vectors], fullFilePath);
        
        // Aktualisieren Sie den Graph und die Beziehungen nach dem Hinzufügen aller Chunks
        await this.updateGraphAndRelationships();
        
      } else {
        throw new Error("Could not embed document chunks!");
      }
  
      return { vectorized: true, error: null };
    } catch (e) {
      debugLog('Error in addDocumentToNamespace', e);
      console.error("addDocumentToNamespace", e.message);
      return { vectorized: false, error: e.message };
    } finally {
      await session.close();
    }
  },

  deleteDocumentFromNamespace: async function(namespace, docId) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (c:Chunk:${namespace} {docId: $docId})
         DETACH DELETE c
         RETURN count(c) as deletedCount`,
        { docId, namespace }
      );
      const deletedCount = result.records[0].get('deletedCount').toNumber();
      console.log(`Deleted ${deletedCount} chunks with docId ${docId} from ${namespace}`);
      return deletedCount > 0;
    } catch (error) {
      return handleError(error, 'Failed to delete document chunks');
    } finally {
      await session.close();
    }
  },

  listDocumentsInNamespace: function(namespace) {
    const session = this.getSession();
    return session.run(
      `MATCH (c:Chunk:${namespace})
       RETURN DISTINCT c.docId AS docId, c.pageContent AS pageContent, c.metadata AS metadata`
    )
      .then((result) => {
        result.records.forEach((record) => {
          log(
            'log',
            `Doc ID: ${record.get('docId')}, Content: ${record.get('pageContent').substring(0, 50)}..., Metadata: ${record.get('metadata')}`
          );
        });
      })
      .catch((error) => handleError(error, 'Failed to list documents in namespace'))
      .finally(() => session.close());
  },

  performEnhancedSimilaritySearch: async function({
    namespace,
    input,
    LLMConnector,
    similarityThreshold = 0.25,
    topN = 4,
    filterFilters = [],
    knnDepth = 2
  }) {
    debugLog('performEnhancedSimilaritySearch called', { namespace, input, similarityThreshold, topN, filterFilters, knnDepth });
    debugLog('filterFilters content:', filterFilters);
    const session = await this.getSession();
    try {
      const namespaceCount = await this.namespaceCount(namespace);
      if (namespaceCount === 0) {
        debugLog('No chunks found in namespace', { namespace });
        return {
          contextTexts: [],
          sources: [],
          scores: [],
          message: `No chunks found in namespace ${namespace}`,
        };
      }

      debugLog('Embedding input text');
      const queryVector = await LLMConnector.embedTextInput(input);
      debugLog('Input text embedded successfully');

      debugLog('Executing enhanced similarity search query');
      const result = await session.run(
        `
        CALL gds.knn.stream('chunkGraph', {
          topK: $topN,
          nodeProperties: ['embedding'],
          similarityMetric: 'cosine'
        })
        YIELD node1, node2, similarity
        WHERE $namespace IN labels(gds.util.asNode(node2))
          AND (size($filterFilters) = 0 OR NOT any(filter IN $filterFilters WHERE toString(node2.docId) = filter))
          AND similarity >= $similarityThreshold
        WITH node2, similarity
        MATCH (node2)-[r:SIMILAR_TO*1..${knnDepth}]-(relatedNode)
        WHERE ALL(rel IN r WHERE rel.similarity >= $similarityThreshold)
        WITH node2, similarity AS directSimilarity,
             collect({node: relatedNode, pathSimilarity: reduce(s = 1.0, rel IN r | s * rel.similarity)}) AS relatedNodes
        WITH node2, directSimilarity,
             relatedNodes,
             reduce(s = 0, x IN relatedNodes | s + x.pathSimilarity) / size(relatedNodes) AS avgKNNScore
        WITH node2, directSimilarity * 0.7 + avgKNNScore * 0.3 AS combinedScore,
             directSimilarity, avgKNNScore, relatedNodes
        ORDER BY combinedScore DESC
        LIMIT $topN
        RETURN node2.pageContent AS contextText,
               node2.metadata AS sourceDocument,
               directSimilarity AS vectorSimilarity,
               avgKNNScore AS knnSimilarity,
               combinedScore,
               [x IN relatedNodes | x.node.pageContent] AS relatedContexts
        `,
        {
          namespace,
          filterFilters,
          topN: neo4j.int(topN * 3),
          queryVector,
          similarityThreshold
        }
      );
      debugLog('Enhanced similarity search query executed', { recordCount: result.records.length });

      const contextTexts = [];
      const sourceDocuments = [];
      const scores = [];
      const relatedContexts = [];

      result.records.forEach(record => {
        contextTexts.push(record.get('contextText'));
        sourceDocuments.push(JSON.parse(record.get('sourceDocument')));
        scores.push(record.get('combinedScore'));
        relatedContexts.push(record.get('relatedContexts'));
      });

      debugLog('Processed enhanced similarity search results', { contextTextsCount: contextTexts.length, scoresCount: scores.length });

      return {
        contextTexts,
        sources: sourceDocuments,
        scores,
        relatedContexts,
        message: contextTexts.length === 0 ? `No results found for namespace ${namespace} above similarity threshold ${similarityThreshold}` : null,
      };
    } catch (error) {
      debugLog('Error in performEnhancedSimilaritySearch', error);
      return handleError(error, 'Enhanced similarity search failed');
    } finally {
      await session.close();
    }
  },

  performSimilaritySearch: async function({
    namespace,
    input,
    LLMConnector,
    similarityThreshold = 0.25,
    topN = 4,
    filterFilters = [],
  }) {
    return this.performEnhancedSimilaritySearch({
      namespace,
      input,
      LLMConnector,
      similarityThreshold,
      topN,
      filterFilters,
    });
  },

  namespaceStats: async function(reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error('namespace required');

    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (c:Chunk:${namespace})
         RETURN count(c) as count`,
        { namespace }
      );
      const count = result.records[0].get('count').toNumber();
      return { vectorCount: count };
    } catch (error) {
      return handleError(error, 'Failed to get namespace stats');
    } finally {
      await session.close();
    }
  },

  deleteDocumentFromNamespace: async function(namespace, docId) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (c:Chunk:${namespace} {docId: $docId})
         DETACH DELETE c
         RETURN count(c) as deletedCount`,
        { docId, namespace }
      );
      const deletedCount = result.records[0].get('deletedCount').toNumber();
      console.log(`Deleted ${deletedCount} chunks with docId ${docId} from ${namespace}`);
      
      if (deletedCount > 0) {
        // Aktualisieren Sie den Graph und die Beziehungen nach dem Löschen von Chunks
        await this.updateGraphAndRelationships();
      }
      
      return deletedCount > 0;
    } catch (error) {
      return handleError(error, 'Failed to delete document chunks');
    } finally {
      await session.close();
    }
  },

  reset: async function() {
    const session = await this.getSession();
    try {
      await session.run('MATCH (n) DETACH DELETE n');
      return { reset: true };
    } catch (error) {
      return handleError(error, 'Failed to reset database');
    } finally {
      await session.close();
    }
  },
};

module.exports.Neo4jDB = Neo4jDB;
