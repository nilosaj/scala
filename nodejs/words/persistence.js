
var oracledb = require('oracledb');
var dbConfig = require('./dbconfig.js');
var redis = require("redis");
total = 0;





exports.retornaPool = function (cb) {
    oracledb.createPool(
        {
            user: dbConfig.user,
            password: dbConfig.password,
            connectString: dbConfig.connectString,
            poolMax: 30, // maximum size of the pool. Increase UV_THREADPOOL_SIZE if you increase poolMax
            poolMin: 1, // start with no connections; let the pool shrink completely
            // poolIncrement: 1, // only grow the pool by one connection at a time
            poolTimeout: 60, // terminate connections that are idle in the pool for 60 seconds
            // poolPingInterval: 60, // check aliveness of connection if in the pool for 60 seconds
            queueRequests: true, // let Node.js queue new getConnection() requests if all pool connections are in use
            queueTimeout: 120000, // terminate getConnection() calls in the queue longer than 60000 milliseconds
            // poolAlias: 'myalias' // could set an alias to allow access to the pool via a name
            // stmtCacheSize: 30 // number of statements that are cached in the statement cache of each connection
            //_enableStats: true
        },
        function (err, pool) {
            if (err) {
                console.error("createPool() error: " + err.message);
                return;
            }
            cb(pool);
        }

    );
}


exports.consultaRedis = function (id, client, indice, pool, callback) {
    client.hgetall(id, function (err, object) {
        if (err){
            console.log("Erro ",err)
        }

        if (object != null) {
           console.log((indice+1)+") Encontrada na cache a palavra '"+object.wordsample+"' para ID "+id)
            callback(object)
        } else {
            console.log((indice+1)+") Não Encontrei o "+id+". Buscando no Oracle....")
            ConsultaOracle(id, pool, indice, (retornoOracle) => {
                client.hmset(retornoOracle.rows[0][0], {
                    "wordsample": retornoOracle.rows[0][1],
                    "word_length": retornoOracle.rows[0][2]
                }, (err, res) => {
                    if (err){
                        console.log("Erro na atualizacao de cache ",err)
                    }
                    callback(res)
                })
               console.log("Encontrada palavra '"+retornoOracle.rows[0][1]+"' . adicionando  ao Redis com indice " + retornoOracle.rows[0][0])

            })

        }

    });

}



function ConsultaOracle(id, pool, indice, callback) {

    // Checkout a connection from the pool
    pool.getConnection(function (err, connection) {
        if (err) {
            console.log('Erro na tentativa de pegar conexao = ', indice, '  ', err);
            return;
        }

        connection.execute(
            " SELECT ID_WORD,WORDSAMPLE , WORD_LENGTH FROM TB_WORDS WHERE  ID_WORD = :id",
            [id], // bind variable value
            function (err, result) {
                if (err) {
                    connection.close(function (err) {
                        if (err) {
                            console.error("Erro na tentativa de liberar a conexão", err);
                        }
                    });
                    console.log('Erro na tentativa de executar a query');
                    return;
                }
                //console.log("Palavra: " + result.rows[0][1] + "  Tamanho: " + result.rows[0][2] + "    Conn open: " + pool.connectionsOpen + " Conn used: " + pool.connectionsInUse);
                connection.close(function (err) {
                    if (err) {
                        console.log('Deu Erro 2');
                    } else {
                       // console.log('Acabou ', result.rows[0][0]);
                        callback(result);
                    }
                });
            }
        );
    });

}



