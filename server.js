var express = require('express'),
	fs = require('fs'),
	request = require('request'),
	cheerio = require('cheerio'),
	app = express(),
	fs = require('fs'),
	csv = require('csvtojson'),
	BASE_URL = 'https://data.gov.in/';

app.get('/scrap', function (req, res) {
	for (let i = 1; i < 7; i++) {
		let url = getNextPageURL(i);
		getCurrentPageHtml(url, function (data) {
			console.log(data)
			if (data) {
				//Can do further code
			}
		});
	}
});
async function getCurrentPageHtml(url, callback) {
	return request(url).catch((err) => {
			logger.error('Http error', err)
			error.logged = true
			throw err
		}).then((response) => {
			console.log(response)
		});

	await request(url, function (error, response, html) {
		if (response) {
			let stateData = getStateData(html),
			urnArray = [];
			if (stateData.length > 0) {
				for (let i = 0; i < stateData.length; i++) {
					let downloadUrl = getUrlToDownloadCSV(stateData[i]);
					csv().fromStream(request.get(downloadUrl))
						.on('csv', jsonObjRow => {
							let urnId = jsonObjRow[0],
							companyName = jsonObjRow[2],
							stateName = jsonObjRow[8];
							//Second Hit
							let finalUrl = 'https://www.zaubacorp.com/company/-/' + urnId;
							getFinalOutput(finalUrl, function (data) {
								console.log(data)
								if (data) {
									callback(urnId);
								}
							});							
						})
						.on('done', (error, response) => {
							console.log('end' + response)
						})
				}
			}
		}
	});
}

async function getFinalOutput(url, callback) {
	return request(url).catch((err) => {
		logger.error('Http error', err)
		error.logged = true
		throw err
	}).then((res) => {
		console.log(res)
	});
	await request(url, function (error, res, html) {
		if (res) {
			var $ = cheerio.load(html);
			if (!error1) {
				var counter = 0;
				$('.table.table-striped').filter(function (item, index) {
					var tableContent = $(this).text();
					$(this).addClass('table_crawal_' + counter);
					// console.log($.html());  //for all tables
					let $$ = cheerio.load($.html());

					var json = '{';
					var otArr = [];
					console.log(".table_crawal_" + counter + " tr");

					var tbl2 = $(".table_crawal_" + counter + " tr").each(function (i) {
						console.log('counter' + counter);
						x = $(this).children();
						var itArr = [];
						x.each(function () {
							itArr.push('"' + $(this).text() + '"');
						});
						otArr.push('"' + i + '": [' + itArr.join(',') + ']');
					})
					json += otArr.join(",") + '}'
					console.log('json read:::' + json);
					counter++;
				})
			}
			callback();
		}
	});
}

function getNextPageURL(pageNo) {
	var pageURL = '/catalog/company-master-data?title=&file_short_format=&page=' + pageNo;
	return BASE_URL + pageURL;
}


function getFinalDataUrl(urnID) {
	var localURL = 'https://www.zaubacorp.com/company/-/' + urnID;
	request(localURL, function (error, response, html) {
		if (!error) {
			$('.company-data.uppercase').filter(function (item, index) {
				var data = $(this);
				// console.log("datadatadatadata:"+data);
			});
		}
	});
}

function getUrlToDownloadCSV(stateName) {
	return BASE_URL + 'sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_' + stateName + ".csv";
}

app.listen('8081')

console.log('Magic happens on port 8081');

exports = module.exports = app;