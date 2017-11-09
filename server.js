var express = require('express'),
	fs = require('fs'),
	request = require('request'),
	cheerio = require('cheerio'),
	app = express(),
	fs = require('fs'),
	csv = require('csvtojson'),
	rp = require('request-promise');
	BASE_URL = 'https://data.gov.in/';

app.get('/scrap', function (req, res) {
    console.time('TIME-TO-SCRAPE');
	startScraping().then(result => {
        console.log('COMPLETED!!!!!');
    	console.timeEnd('TIME-TO-SCRAPE');
	})
});

async function startScraping() {
	let output = [];
	for (let i = 0; i <= 1; i++) {
		let url = getNextPageURL(i);
		let records =  await crawling(url,i);
		output.push(records);
		console.log('Output Length  ' + output.length);
	}
	for(element of output){
		console.log('Sheets  ' + element.length);
		for(sheets of element) {
			console.log('Records  ' + sheets.length);
			let count = 0;
			for(record of sheets) {
				// if(['Mizoram','Lakshadweep'].indexOf(record.stateName) > -1)
				// {
				console.log('URN     ' + count + '    ' + record.stateName + '    ---    ' + record.urnId);
				let finalJson = await getPageDetailsUsingUrnId(record.urnId);
				//console.log(finalJson);
				count++;
				//}
			}
		}		
	}
};

async function crawling(url,index) {
	let stateArray = await getCurrentPageHtml(url);	
	let stateUrns = [];
	if (stateArray.length > 0){
		for( let i = 0; i < stateArray.length; i++ ) {
			console.log('New State   ',stateArray[i]);
			let csvRecords = await getCSVRecords(stateArray[i]);
			stateUrns.push(csvRecords);
			console.log(csvRecords.length);			
		}
		return stateUrns;
	}
}
function getPageDetailsUsingUrnId(urnId)
{
	let url = 'https://www.zaubacorp.com/company/-/'+ urnId;
	let options = {
		uri: url,
		transform: function (html) {
			return cheerio.load(html);
		}
	};
	
	return rp(options).then(function ($) {
			let counter = 0,
				json ,
				array = [];
			$('.table.table-striped').each(function(item, index) {
				$(this).addClass('table_crawal_' + counter);
				otArr   = [];
				json    = '{';
				let tbl2 = $(".table_crawal_" + counter + " tr").each(function(i) {
					x = $(this).children();
					let itArr = [],
						inc;		
						x.each(function( inc ) {
							let tableValue = $(this).text().replace(/\s+/g, '_').toLowerCase();
							if ( inc === 0 ) {
								itArr.push( tableValue );
							} else {
								itArr.push('"' + $(this).text() + '"');
							}
						});
					otArr.push( itArr.join(':') );
					inc++;
				});
				json += otArr.join(",") + '}';
				array.push( json )
				counter++;
			});
			if( array ){
				return array;
			}
		})
		.catch(function (err) {
			console.log(err);
		});										
}

function getCSVRecords(state){
	let downloadUrl = urlToDownloadCSV(state);
	return new Promise(function(resolve, reject) {
		let arr = [];
		csv().fromStream(
					request.get(downloadUrl) // Request
					).on( 'csv' , (jsonObjRow) => {
						let value = {
							urnId 		: jsonObjRow[0],
							companyName : jsonObjRow[2],
							stateName	: jsonObjRow[8]
						};
						arr.push(value);	
					})
					.on('end', function() {
						resolve(arr);
					});
		});
}

function getCurrentPageHtml(url) {
	var options = {uri: url, transform: function (body) {
			return cheerio.load(body);
		}
	};	
	return rp(options).then(function ($) {
					let stateArray 	= [];
					$('.data-extension.csv').filter( function( item,index ){
						var data 				= 	$(this),
							local				=	data['0'].attribs.href.split('data')[2].split('-'),
							fullURL				=	BASE_URL+data['0'].attribs.href,
							index1 				= 	fullURL.indexOf('-data-'),
							index2 				= 	fullURL.indexOf('-upto-'),
							finalStateName 		= 	fullURL.substring(index1 + 6,index2),
							arrStateName 		= 	finalStateName.split('-'),
							arrStateNameLength 	= 	arrStateName.length,
							stateNameWithConcat = 	'';

						for ( var j = 0; j <= arrStateNameLength; j++ ) {
							if ( j === arrStateNameLength ) {
								stateNameWithConcat = stateNameWithConcat.substring(0,stateNameWithConcat.length-1);
								
								stateArray.push( stateNameWithConcat );
							} else {
								stateNameWithConcat += arrStateName[j].charAt(0).toUpperCase() + arrStateName[j].slice(1) + '_';
							}
						}						
					});
					return stateArray;
		})
		.catch(function (err) {
			// Crawling failed or Cheerio choked...
		});
}

function urlToDownloadCSV(stateName) {
	return BASE_URL+'sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_'+stateName+".csv";
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