const TelegramBot = require('node-telegram-bot-api');

const token = '368750321:AAE1Ldh4l3pzqe_uhFP70soVFSCcCCfaa_Q';

const bot = new TelegramBot(token);



exports.enviaMensagem = function enviaMensagem(mensagem){
    bot.sendMessage('-204773379', mensagem);
}