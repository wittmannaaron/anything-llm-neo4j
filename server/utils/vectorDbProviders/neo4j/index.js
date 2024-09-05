require('dotenv').config();

const neo4j = require('neo4j-driver');
const { getEmbeddingEngineSelection } = require('../../helpers');

const log = (level, message, ...args) => {
  console[level](`Neo4j::${message}`, ...args);
};

const handleError = (error, customMessage) => {
  log('error', `${customMessage} - ${error.message}`);
  return { error: error.message };
};

const Neo4jDB = {
  name: 'Neo4j',
  driver: null,

  initialize: async function() {
    if (process.env.VECTOR_DB !== 'neo4j') {
      throw new Error('Invalid ENV settings');
    }

    if (!process.env.NEO4J_URI || !process.env.NEO4J_USER || !process.env.NEO4J_PASSWORD) {
      throw new Error('Missing required environment variables');
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
      await this.createEmbeddingIndex();
    } catch (error) {
      throw handleError(error, 'Connection failed');
    }
  },

  createEmbeddingIndex: async function() {
    const session = await this.getSession();
    try {
      await session.run(`
        CREATE INDEX embedding_index IF NOT EXISTS
        FOR (d:Document)
        ON (d.embedding)
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
        `MATCH (d:Document:${namespace}) RETURN count(d) as count LIMIT 1`
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
        `MATCH (d:Document:${namespace}) RETURN count(d) as count`
      );
      return result.records[0].get('count').toNumber();
    } catch (error) {
      return handleError(error, 'Failed to count namespace');
    } finally {
      await session.close();
    }
  },

  addDocumentToNamespace: async function(namespace, documentData) {
    const session = await this.getSession();
    try {
      const { docId, pageContent, ...metadata } = documentData;
      if (!docId || typeof docId !== 'string') {
        throw new Error('Invalid or missing docId in document data');
      }

      const embedder = getEmbeddingEngineSelection();
      const embedding = await embedder.embedTextInput(pageContent);
      if (!embedding) {
        throw new Error('Failed to generate embedding');
      }

      await session.run(
        `CREATE (d:Document:${namespace} {
          docId: $docId, pageContent: $pageContent, metadata: $metadata, embedding: $embedding
        })`,
        { docId, pageContent, metadata: JSON.stringify(metadata), embedding }
      );

      log('log', `Document added to ${namespace} with docId ${docId}`);
      return { vectorized: true, error: null };
    } catch (error) {
      return handleError(error, 'Failed to add document');
    } finally {
      await session.close();
    }
  },

  deleteDocumentFromNamespace: async function(namespace, docId) {
    const session = await this.getSession();
    try {
      log('log', `Attempting to delete document with docId ${docId} from ${namespace}`);
      
      const result = await session.run(
        `MATCH (d:Document:${namespace} {docId: $docId})
         DETACH DELETE d
         RETURN count(d) as deletedCount`,
        { docId }
      );
      const deletedCount = result.records[0].get('deletedCount').toNumber();
      log('log', `Deleted ${deletedCount} nodes with docId ${docId} from ${namespace}`);
      
      return deletedCount > 0;
    } catch (error) {
      return handleError(error, 'Failed to delete document');
    } finally {
      await session.close();
    }
  },

  listDocumentsInNamespace: function(namespace) {
    const session = this.getSession();
    return session.run(
      `MATCH (d:Document:${namespace})
       RETURN d.docId AS docId, d.pageContent AS pageContent, d.metadata AS metadata`
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

  performSimilaritySearch: async function({
    namespace,
    input,
    LLMConnector,
    similarityThreshold = 0.25,
    topN = 4,
    filterFilters = [],
  }) {
    const session = await this.getSession();
    try {
      const namespaceCount = await this.namespaceCount(namespace);
      if (namespaceCount === 0) {
        return {
          contextTexts: [],
          sourceDocuments: [],
          scores: [],
          message: `No documents found in namespace ${namespace}`,
        };
      }

      const queryVector = await LLMConnector.embedTextInput(input);
      const limitValue = neo4j.int(Math.floor(topN));

      const result = await session.run(
        `MATCH (d:Document:${namespace})                                                                                    
         WHERE ALL(filter IN $filterFilters WHERE NOT d.docId IN filter)                                                   
         WITH d,                                                                                                            
         gds.similarity.cosine(d.embedding, $queryVector) AS similarity                                                     
         RETURN d.pageContent AS contextText, d.metadata AS sourceDocument, similarity                                      
         ORDER BY similarity DESC
         LIMIT $limitValue`,
        { namespace, queryVector, filterFilters, limitValue }
      );

      const contextTexts = [];
      const sourceDocuments = [];
      const scores = [];

      result.records.forEach((record) => {
        contextTexts.push(record.get('contextText'));
        const sourceDocument = JSON.parse(record.get('sourceDocument'));
        sourceDocuments.push({
          ...sourceDocument,
          text: record.get('contextText'),
        });
        scores.push(record.get('similarity'));
      });

      return {
        contextTexts,
        sources: sourceDocuments,
        scores,
        message: contextTexts.length === 0 ? `No results found for namespace ${namespace}` : null,
      };
    } catch (error) {
      return handleError(error, 'Similarity search failed');
    } finally {
      await session.close();
    }
  },

  namespaceStats: async function(reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error('namespace required');

    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace})
         RETURN count(d) as count`,
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

  deleteNamespace: async function(reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error('namespace required');

    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace})
         DETACH DELETE d
         RETURN count(d) as deletedCount`,
        { namespace }
      );
      const deletedCount = result.records[0].get('deletedCount').toNumber();
      return {
        message: `Namespace ${namespace} was deleted along with ${deletedCount} vectors.`,
      };
    } catch (error) {
      return handleError(error, 'Failed to delete namespace');
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
