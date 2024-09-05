require("dotenv").config();

// Erforderliche Module importieren
const neo4j = require('neo4j-driver'); // Neo4j Treiber für Node.js
const { v4: uuidv4 } = require("uuid"); // Für die Generierung einzigartiger IDs
const { getEmbeddingEngineSelection } = require("../../helpers");

// Hauptobjekt für die Neo4j-Adapter-Funktionalität
const Neo4jDB = {
  name: "Neo4j", // Name des Adapters
  driver: null, // Speichert die Neo4j-Treiberinstanz

  // Initialisiert die Verbindung zur Neo4j-Datenbank
  initialize: async function() {
    // Überprüft, ob die richtige Umgebungsvariable gesetzt ist
    if (process.env.VECTOR_DB !== "neo4j")
      throw new Error("Neo4j::Invalid ENV settings");

    // Überprüft, ob alle erforderlichen Umgebungsvariablen vorhanden sind
    if (!process.env.NEO4J_URI || !process.env.NEO4J_USER || !process.env.NEO4J_PASSWORD) {
      throw new Error("Neo4j::Missing required environment variables");
    }

    console.log("Neo4j::Attempting connection with:", {
      uri: process.env.NEO4J_URI,
      user: process.env.NEO4J_USER,
      // Wir loggen das Passwort nicht aus Sicherheitsgründen
    });

    // Erstellt eine neue Treiberinstanz mit den Verbindungsdetails
    this.driver = neo4j.driver(
      process.env.NEO4J_URI,
      neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
    );

    try {
      // Überprüft die Verbindung zur Datenbank
      await this.driver.verifyConnectivity();
      console.log("Neo4j::Connection established");

      // Erstellt einen Index für die Embedding-Felder
      await this.createEmbeddingIndex();
    } catch (error) {
      console.error("Neo4j::Connection failed - " + error.message);
      throw error;
    }
  },

  // Erstellt einen Index für die Embedding-Felder
  createEmbeddingIndex: async function() {
    const session = await this.getSession();
    try {
      await session.run(`
        CREATE INDEX embedding_index IF NOT EXISTS
        FOR (d:Document)
        ON (d.embedding)
      `);
      console.log("Neo4j::Embedding index created or already exists");
    } catch (error) {
      console.error("Neo4j::Failed to create embedding index - " + error.message);
    } finally {
      await session.close();
    }
  },

  // Trennt die Verbindung zur Neo4j-Datenbank
  disconnect: async function() {
    if (this.driver) {
      await this.driver.close();
      this.driver = null;
    }
  },

  // Gibt eine neue Sitzung zurück
  getSession: async function() {
    if (!this.driver) {
      await this.initialize();
    }
    return this.driver.session();
  },

  // Überprüft, ob die Verbindung zur Datenbank aktiv ist
  heartbeat: async function() {
    const session = await this.getSession();
    try {
      await session.run("RETURN 1");
      return { heartbeat: true };
    } catch (error) {
      console.error("Neo4j::Heartbeat failed - " + error.message);
      return { heartbeat: false, error: error.message };
    } finally {
      await session.close();
    }
  },

  // Überprüft, ob ein Namespace existiert
  hasNamespace: async function(namespace) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace}) RETURN count(d) as count LIMIT 1`
      );
      const count = result.records[0].get("count").toNumber();
      return count > 0;
    } catch (error) {
      console.error(`Neo4j::Failed to check namespace - ${error.message}`);
      return false;
    } finally {
      await session.close();
    }
  },

  // Zählt die Anzahl der Dokumente in einem Namespace
  namespaceCount: async function(namespace) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace}) RETURN count(d) as count`
      );
      return result.records[0].get("count").toNumber();
    } catch (error) {
      console.error(`Neo4j::Failed to count namespace - ${error.message}`);
      return 0;
    } finally {
      await session.close();
    }
  },

  // Fügt ein Dokument zu einem bestimmten Namespace hinzu
  addDocumentToNamespace: async function (
    namespace,
    documentData,
    fullFilePath
  ) {
    const session = await this.getSession();
    try {
      // Extrahiert die benötigten Daten aus documentData
      const { id, pageContent, ...metadata } = documentData;

      // Ensure that id is present and valid
      if (!id || typeof id !== 'string') {
        throw new Error("Invalid or missing id in document data");
      }

      // Holt den konfigurierten Embedder
      const embedder = getEmbeddingEngineSelection();

      // Erstellt das Embedding
      const embedding = await embedder.embedTextInput(pageContent);

      // Überprüft, ob das Embedding vorhanden ist
      if (!embedding || !Array.isArray(embedding)) {
        throw new Error("Embedding vector is missing or not an array");
      }

      // Erstellt einen neuen Knoten in der Datenbank
      await session.run(
        `CREATE (d:Document:${namespace} {
          doc_id: $docId,
          pageContent: $pageContent,
          metadata: $metadata,
          embedding: $embedding
        })`,
        {
          docId: id, // Use the original id from the document
          pageContent: pageContent,
          metadata: JSON.stringify(metadata), // Metadaten als JSON-String
          embedding: embedding // Speichert den tatsächlichen Embedding-Vektor
        }
      );

      console.log(`Neo4j::Document added to ${namespace} with id ${id}`);
      return { vectorized: true, error: null };
    } catch (error) {
      console.error(`Neo4j::Failed to add document - ${error.message}`);
      return { vectorized: false, error: error.message };
    } finally {
      await session.close();
    }
  },

  // Löscht ein Dokument aus einem bestimmten Namespace
  deleteDocumentFromNamespace: async function (namespace, docId) {
    const session = await this.getSession();
    try {
      console.log(`Neo4j::Attempting to delete document with id ${docId} from ${namespace}`);
      
      // Log all documents in the namespace before deletion
      console.log("Documents in namespace before deletion:");
      await this.listDocumentsInNamespace(namespace);
      
      const result = await session.run(
        `MATCH (d:Document:${namespace} {doc_id: $docId})
         DETACH DELETE d
         RETURN count(d) as deletedCount`,
        { docId }
      );
      const deletedCount = result.records[0].get('deletedCount').toNumber();
      console.log(`Neo4j::Deleted ${deletedCount} nodes with docId ${docId} from ${namespace}`);
      
      // Log all documents in the namespace after deletion
      console.log("Documents in namespace after deletion:");
      await this.listDocumentsInNamespace(namespace);
      
      return deletedCount > 0;
    } catch (error) {
      console.error(`Neo4j::Failed to delete document - ${error.message}`);
      return false;
    } finally {
      await session.close();
    }
  },

  listDocumentsInNamespace: async function(namespace) {
    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace})
         RETURN d.doc_id AS docId, d.pageContent AS pageContent, d.metadata AS metadata`
      );
      result.records.forEach((record) => {
        console.log(`Doc ID: ${record.get("docId")}, Content: ${record.get("pageContent").substring(0, 50)}..., Metadata: ${record.get("metadata")}`);
      });
    } catch (error) {
      console.error(`Neo4j::Failed to list documents in namespace - ${error.message}`);
    } finally {
      await session.close();
    }
  },

  // Führt eine Ähnlichkeitssuche in einem Namespace durch
  performSimilaritySearch: async function ({
    namespace,
    input,
    LLMConnector,
    similarityThreshold = 0.25,
    topN = 4,
    filterFilters = []
  }) {
    const session = await this.getSession();
    try {
      // Überprüft, ob der Namespace existiert und Dokumente enthält
      const namespaceCount = await this.namespaceCount(namespace);
      // console.log(`Debug: Namespace ${namespace} contains ${namespaceCount} documents`);

      if (namespaceCount === 0) {
        return {
          contextTexts: [],
          sourceDocuments: [],
          scores: [],
          message: `No documents found in namespace ${namespace}`,
        };
      }

      // Generiert den Embedding-Vektor für die Eingabe
      const queryVector = await LLMConnector.embedTextInput(input);

      // Stellt sicher, dass topN eine ganze Zahl ist
      const limitValue = neo4j.int(Math.floor(topN));

      // console.log("Debug: limitValue =", limitValue);
      // console.log("Debug: similarityThreshold =", similarityThreshold);

      // Add a function to fetch and log embeddings from the database
      async function logStoredEmbeddings(session, namespace) {
        try {
          const result = await session.run(
            `MATCH (d:Document:${namespace}) RETURN d.doc_id AS docId, d.embedding AS embedding`
          );
          result.records.forEach((record) => {
            // console.log(
            //   `Debug: Stored Embedding for docId ${record.get("docId")}:`,
            //   record.get("embedding")
            // );
          });
        } catch (error) {
          console.error(`Neo4j::Failed to fetch embeddings - ${error.message}`);
        }
      }

      // Führt die Ähnlichkeitssuche in der Datenbank durch
      // console.log("Debug: Query Vector", queryVector);
      // Call this function before running the similarity search
      await logStoredEmbeddings(session, namespace);
      const result = await session.run(
        `MATCH (d:Document:${namespace})                                                                                    
         WHERE ALL(filter IN $filterFilters WHERE NOT d.doc_id IN filter)                                                   
         WITH d,                                                                                                            
         gds.similarity.cosine(d.embedding, $queryVector) AS similarity                                                     
         RETURN d.pageContent AS contextText, d.metadata AS sourceDocument, similarity                                      
         ORDER BY similarity DESC`,
        { namespace, queryVector, filterFilters }
      );

      result.records.forEach((record) => {
        // console.log(`Debug: Document ${record.get("sourceDocument")} has similarity ${record.get("similarity")}`); 
      });

      // Verarbeitet die Ergebnisse
      const contextTexts = [];
      const sourceDocuments = [];
      const scores = [];

      result.records.forEach(record => {
        contextTexts.push(record.get("contextText"));
        const sourceDocument = JSON.parse(record.get("sourceDocument"));
        sourceDocuments.push({ ...sourceDocument, text: record.get("contextText") });
        scores.push(record.get("similarity"));
      });

      // console.log("Debug: Search results", {
      //   contextTextsLength: contextTexts.length,
      //   sourceDocumentsLength: sourceDocuments.length,
      //   scoresLength: scores.length
      // });

      return {
        contextTexts,
        sources: sourceDocuments,
        scores,
        message: contextTexts.length === 0 ? `No results found for namespace ${namespace}` : null,
      };
    } catch (error) {
      console.error(`Neo4j::Similarity search failed - ${error.message}`);
      return {
        contextTexts: [],
        sourceDocuments: [],
        scores: [],
        error: error.message
      };
    } finally {
      await session.close();
    }
  },

  // Gibt Statistiken für einen Namespace zurück
  "namespace-stats": async function (reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error("namespace required");

    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace})
         RETURN count(d) as count`,
        { namespace }
      );
      const count = result.records[0].get("count").toNumber();
      return { vectorCount: count };
    } catch (error) {
      console.error(`Neo4j::Failed to get namespace stats - ${error.message}`);
      return { error: error.message };
    } finally {
      await session.close();
    }
  },

  // Löscht einen gesamten Namespace
  "delete-namespace": async function (reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error("namespace required");

    const session = await this.getSession();
    try {
      const result = await session.run(
        `MATCH (d:Document:${namespace})
         DETACH DELETE d
         RETURN count(d) as deletedCount`,
        { namespace }
      );
      const deletedCount = result.records[0].get("deletedCount").toNumber();
      return {
        message: `Namespace ${namespace} was deleted along with ${deletedCount} vectors.`,
      };
    } catch (error) {
      console.error(`Neo4j::Failed to delete namespace - ${error.message}`);
      return { error: error.message };
    } finally {
      await session.close();
    }
  },

  // Setzt die gesamte Datenbank zurück (löscht alle Daten)
  reset: async function () {
    const session = await this.getSession();
    try {
      await session.run("MATCH (n) DETACH DELETE n");
      return { reset: true };
    } catch (error) {
      console.error(`Neo4j::Failed to reset database - ${error.message}`);
      return { reset: false, error: error.message };
    } finally {
      await session.close();
    }
  }
};

// Exportiert das Neo4jAdapter-Objekt für die Verwendung in anderen Modulen
module.exports.Neo4jDB = Neo4jDB;
