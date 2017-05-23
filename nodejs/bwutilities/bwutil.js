var _ = require('lodash');
var yargs = require('yargs');
var bwfs = require('./bwfiles');
var bwcomm = require('./bwnotification.js');


var inputargs = yargs.argv;
var listaArquivosGrandes = [];

if (inputargs._[0]==='verificalogs'){
   verificaLogs()
   
}else{
    operacaoNaoIdentificada()
}


function verificaLogs(){
    console.log(`[######### INICIANDO VERIFICACAO DE LOGS  NO DIRETORIO  ${inputargs._[1]} #########]`)
    bwfs.verificaDiretorio(inputargs._[1],inputargs._[2],(retorno)=>{
       listaArquivosGrandes = retorno //melhorar  aqui
       listaArquivosgrandes()
    });
};


function listaArquivosgrandes(){
    listaArquivosGrandes.forEach(item=>{
        var msg = 'ARQUIVO: '+item['caminho']+' TAMANHO: ['+item['tamanho']+']';
        bwcomm.enviaMensagem(msg);
    })
};

function operacaoNaoIdentificada(){
    console.log('[Operacao não identificada]');
    console.log(' ');
    console.log('Operações disponiveis :');
    console.log('verificalogs <X> <Y> - verifica logs no caminho X com tamanho acima de Y em Mb');
    console.log(' ');
    

};



