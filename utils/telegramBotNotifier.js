const axios = require('axios')

module.exports = {
  botApiKey: process.env.TELEGRAM_NOTIFIER_BOT_API_KEY,
  botChatId: process.env.TELEGRAM_NOTIFIER_BOT_GROUP_CHAT_ID,
  async notify (text) {
    return axios.get(`https://api.telegram.org/bot${this.botApiKey}/sendMessage?chat_id=${this.botChatId}&text=${text}`)
  }
}
