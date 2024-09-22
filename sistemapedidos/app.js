const express = require('express');
const { PubSub } = require('@google-cloud/pubsub');
const client = require('./db'); 

const app = express();
const pubSubClient = new PubSub({
    projectId: 'serjava-demo',
    keyFilename: './serjava-demo-1aceb58abd45.json'  
});

const subscriptionName = 'projects/serjava-demo/subscriptions/marketing-sub'; 




app.get('/orders', async (req, res) => {
    const { uuid, cliente_id, produto_id } = req.query;

    let query = 'SELECT * FROM pedido WHERE 1=1';
    const params = [];

    if (uuid) {
        query += ' AND uuid = $1';
        params.push(uuid);
    }
    if (cliente_id) {
        query += ' AND cliente_id = $2';
        params.push(cliente_id);
    }
    if (produto_id) {
        query += ' AND id IN (SELECT pedido_id FROM item_do_pedido WHERE produto_id = $3)';
        params.push(produto_id);
    }

    try {
        const result = await client.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).send('Erro ao consultar pedidos');
    }
});


async function listenForMessages() {
    const subscription = pubSubClient.subscription(subscriptionName);


    const messageHandler = async (message) => {
        console.log(`Mensagem recebida: ${message.id}`);

        const messageData = message.data.toString();
        console.log(`Dados da mensagem: ${messageData}`);
        const cleanedData = messageData


        
        data = JSON.parse(cleanedData);

        if (message.data.startsWith('{') && message.data.endsWith('}')) {
            data = JSON.parse(cleanedData);
        }
        
        const { customer, items, uuid, created_at } = data

       

        try {

            
            const clienteResult = await client.query(
                'INSERT INTO cliente (nome) VALUES ($1) ON CONFLICT (id) DO NOTHING RETURNING id',
                [customer.name]
            );

            
            const clienteId = clienteResult.rows.length > 0 ? clienteResult.rows[0].id : customer.id;

            
            const valorTotal = items.reduce((total, item) => {
                return total + (item.sku.value * item.quantity);
            }, 0);

           
            const pedidoResult = await client.query(
                `INSERT INTO pedido (uuid, cliente_id, created_at, valor_total) 
                 VALUES ($1, $2, $3, $4) 
                 ON CONFLICT (uuid) DO UPDATE SET 
                 cliente_id = EXCLUDED.cliente_id,
                 created_at = EXCLUDED.created_at,
                 valor_total = EXCLUDED.valor_total
                 RETURNING id`,
                [uuid, clienteId, created_at, valorTotal]
            );
            
            const pedidoId = pedidoResult.rows[0].id;

            
            for (const item of items) {
                const produtoId = parseInt(item.sku.id, 10); 
                if (isNaN(produtoId)) {
                    console.error(`Produto ID inválido: ${item.sku.id}`);
                    continue; 
                }
                await client.query(
                    'INSERT INTO item_do_pedido (pedido_id, produto_id, quantidade) VALUES ($1, $2, $3)',
                    [pedidoId, produtoId, item.quantity]
                );
            }
            
            message.ack();
            console.log('Mensagem processada e armazenada com sucesso');
        } catch (err) {
            console.error('Erro ao armazenar mensagem:', err);
            message.nack();  
        }
    };

    subscription.on('message', messageHandler);
    console.log(`Escutando mensagens na assinatura: ${subscriptionName}`);
}


listenForMessages().catch(console.error);


const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});
