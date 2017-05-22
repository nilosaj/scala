var _ = require('lodash');
var bwfs = require('./bwfiles');
var inputargs = process.argv


if (inputargs[2]==='verificalogs'){
    verificaLogs();
}else{
    operacaoNaoIdentificada()
}


function verificaLogs(){
    console.log(`iniciando verificalogs  em  ${inputargs[3]}`)
    bwfs.verificaDiretorio(inputargs[3],inputargs[4]);
};


function operacaoNaoIdentificada(){
    console.log('[Operacao não identificada]');
    console.log(' ');
    console.log('Operações disponiveis :');
    console.log('verificalogs <X> <Y> - verifica logs no caminho X com tamanho acima de Y em Mb');
    console.log(' ');
    

};

