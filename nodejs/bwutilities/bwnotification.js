const TelegramBot = require('node-telegram-bot-api');
var config = require('./config.json');

const bot = new TelegramBot(config.token);


exports.enviaMensagem = function enviaMensagem(mensagem){
    bot.sendMessage(config.chatId, mensagem);
}