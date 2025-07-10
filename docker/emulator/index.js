const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

require('dotenv').config();

const app = express();
const PORT = 8545;
const NODE_URL = process.env.UPSTREAM_RPC || '<YOUR_FALLBACK_UPSTREAM_RPC>';

app.use(bodyParser.json());

async function getBlockReceipts(block) {
  try {
    const { data: blockData } = await axios.post(NODE_URL, {
      jsonrpc: '2.0',
      method: 'eth_getBlockByNumber',
      params: [block, false],
      id: 1,
    });

    const transactions = blockData.result?.transactions || [];

    const receiptPromises = transactions.map((tx) =>
      axios.post(NODE_URL, {
        jsonrpc: '2.0',
        method: 'eth_getTransactionReceipt',
        params: [tx],
        id: 1,
      })
    );

    const receiptsRaw = await Promise.all(receiptPromises);

    const receipts = receiptsRaw.map(({ data }) => {
      const txReceipt = data.result;

      // Normalize logs
      for (let log of txReceipt.logs) {
        let logData = {
          address: log.address,
          topics: log.topics,
          data: log.data,
          blockNumber: log.blockNumber,
          transactionHash: log.transactionHash,
          transactionIndex: log.transactionIndex,
          blockHash: log.blockHash,
          logIndex: log.logIndex,
          removed: log.removed,
          id: log.id,
        };
        txReceipt.logs = logData;
      }

      return txReceipt;
    });

    if (receipts.length === 0) {
      console.warn(`No receipts found for block ${block}`);
    }
    return receipts;
  } catch (err) {
    console.error('Error emulating eth_getBlockReceipts:', err.message);
    return null;
  }
}

// JSON-RPC handler
app.post('/', async (req, res) => {
  const { method, params, id } = req.body;

  if (method === 'eth_getBlockReceipts') {
    const [blockHashOrNumber] = params;
    try {
      const receipts = await getBlockReceipts(blockHashOrNumber);
      return res.json({ jsonrpc: '2.0', result: receipts, id });
    } catch (err) {
      console.error(err);
      return res.status(500).json({
        jsonrpc: '2.0',
        id,
        error: { code: -32000, message: 'Failed to emulate block receipts' },
      });
    }
  }

  // Fallback: proxy other calls to the real RPC
  try {
    const response = await axios.post(NODE_URL, req.body);
    res.json(response.data);
  } catch (err) {
    console.error('Upstream RPC error:', err.message);
    res.status(502).json({
      jsonrpc: '2.0',
      error: { code: -32000, message: 'Upstream RPC failed' },
      id,
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Emulator listening on http://localhost:${PORT}`);
});
