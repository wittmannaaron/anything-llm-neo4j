require('dotenv').config();
const { Neo4jDB } = require('./index');
const { getEmbeddingEngineSelection } = require('../../helpers');

async function testNeo4jAdapter() {
  try {
    // Initialize the Neo4jDB adapter
    await Neo4jDB.initialize();

    const namespace = 'test_namespace';
    const docId = 'test_doc_id';
    const documentData = {
      id: docId,
      pageContent: 'This is a test document.',
      metadata: {
        source: 'test',
        author: 'tester'
      }
    };

    console.log('Adding document to namespace...');
    const addResult = await Neo4jDB.addDocumentToNamespace(namespace, documentData);
    console.log('Add result:', addResult);

    console.log('\nAttempting to delete the document...');
    const deleteResult = await Neo4jDB.deleteDocumentFromNamespace(namespace, docId);
    console.log('Delete result:', deleteResult);

    // Disconnect from the database
    await Neo4jDB.disconnect();
  } catch (error) {
    console.error('Error during test:', error);
  }
}

testNeo4jAdapter();