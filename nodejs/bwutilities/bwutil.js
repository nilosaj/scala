var _ = require('lodash');
var yargs = require('yargs');

var bwfs = require('./bwfiles');
var bwcomm = require('./bwnotification.js');

var inputargs = yargs.argv;

function removeNull (item) {
    return item !== null;
}

var listarArquivosGrandes = function (arquivosGrandes) {
    arquivosGrandes = arquivosGrandes.filter(removeNull);
    arquivosGrandes.forEach((item) => {

        if (item) {
            var msg = 'ARQUIVO: ' + item['caminho'] + ' TAMANHO: [' + item['tamanho'] + ']';
            console.log(msg);
        }
        var msg = 'ARQUIVO: ' + item['caminho'] + ' TAMANHO: [' + item['tamanho'] + ']';
        bwcomm.enviaMensagem(msg);
    })
};

function verificaLogs() {
    console.log(`[######### INICIANDO VERIFICACAO DE LOGS  NO DIRETORIO  ${inputargs._[1]} #########]`)
    bwfs.verificaDiretorio(inputargs._[1], inputargs._[2])
        .then((arquivosGrandes) => {
            listarArquivosGrandes(arquivosGrandes);
        }, (err) => {
            console.log("err verificaDiretorio", err);
        });
}

function operacaoNaoIdentificada() {
    console.log('[Operacao não identificada]');
    console.log(' ');
    console.log('Operações disponiveis :');
    console.log('verificalogs <X> <Y> - verifica logs no caminho X com tamanho acima de Y em Mb');
    console.log(' ');
}

if (inputargs._[0] === 'verificalogs') {
    verificaLogs()

} else {
    operacaoNaoIdentificada()
}
