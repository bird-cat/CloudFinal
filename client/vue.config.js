const fs = require('fs')
module.exports = {
	configureWebpack:{
		optimization: {
			splitChunks: {
				minSize: 10000,
				maxSize: 250000,
			}
		}
	},
	"transpileDependencies": [
		"vuetify"
	],
	devServer: {
		port: 8080,
		https: {
			key: fs.readFileSync('server.key'),
			cert: fs.readFileSync('server.crt'),
		},
	}
}
