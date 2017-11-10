var express = require('express'),
fs = require('fs'),
request = require('request'),
cheerio = require('cheerio'),
app = express(),
csv = require('csvtojson'),
rp = require('request-promise');
BASE_URL = 'https://data.gov.in/';

const Sequelize = require('sequelize');
const sequelize = new Sequelize('webcrawler', 'root', 'admin', {
host: 'localhost',
dialect: 'mysql',
operatorsAliases: false
});

app.get('/scrap', function (req, res) {
console.time('TIME-TO-SCRAPE');
startScraping().then(result => {
	console.log('COMPLETED!!!!!');
	console.timeEnd('TIME-TO-SCRAPE');
})
});

async function startScraping() {
let output = [];

//get all urns per state.
for (let i = 0; i <= 5; i++) {
	let url = getNextPageURL(i);
	let records =  await crawling(url,i);
	output.push(records);
	console.log('Output Length  ' + output.length);
}

//process urn state wise and store result in finalJSON
let finalStateJsonArray = [];

for( element of output ){
	for(sheets of element) {
		let count = 0;
		for(record of sheets) {
			if(record.stateName === 'Lakshadweep'){
				console.log('URN     ' + count + '    ' + record.stateName + '    ---    ' + record.urnId);
				let finalJson = await getPageDetailsUsingUrnId( record.urnId );

				//DB Entry
				let stateName = record.stateName;
				let stateTableSchema = createTableByStateName( stateName );
				let inc = {};

				finalJson.forEach( function( tableData ){
					let mergedObject = Object.assign( inc, tableData );
				});

				//console.log(inc);

				if( inc ){
					finalStateJsonArray.push({State: record.stateName, Json : inc});

					//console.log(finalStateJsonArray);
					// sequelize.sync()
					//     .then(() => stateTableSchema.create( inc ) );
				}

				//console.log('DB Entry Done');
				//DB Entry

				count++;

			} else {
			   //console.log(record.stateName);
			}

		}

		console.log('Array     ' + finalStateJsonArray )
	}

}

debugger
//make entry of final json in DB

};


async function crawling(url,index) {
let stateArray = await getCurrentPageHtml(url);
let stateUrns = [];
if (stateArray.length > 0){
	for( let i = 0; i < stateArray.length; i++ ) {
		console.log('New State   ',stateArray[i]);
		let csvRecords = await getCSVRecords(stateArray[i]);
		stateUrns.push(csvRecords);
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
	let tableDataArray = [],
		data;
	$('.company-data.uppercase').filter(function( item, index ) {
		let tableName = $(this).text().replace(/\s+/g, '_').toLowerCase();

		switch ( tableName ){
			case 'company_details':
				data = $(this).next().html();
				tableDataArray.push( jsonArray( data, $ ) );
				break;
			case 'share_capital_&_number_of_employees':
				data = $(this).next().html();
				tableDataArray.push( jsonArray( data, $ ) );
				break;
			case '_listing_and_annual_compliance_details':
				data = $(this).next().html();
				tableDataArray.push( jsonArray( data, $ ) );
				break;
			case 'contact_details':
				data = $(this).next().html();
				tableDataArray.push( getJsonFromDiv( data, $ ) );
				break;
			case '_director_details':
				data = $(this).next().html();
				tableDataArray.push( getDirectoDetails( data, $ ) );
				break;
			default:
				//console.log( 'No matching table found.')
				break;
		}
	});
	return tableDataArray;
})
	.catch(function (err) {
		//console.log(err);
	});
}

function getDirectoDetails( data, $ ){
let object = {},
	directorNames = '';

	$( data ).children("tbody tr").map(function() {
		x = $(this).children();
		x.each(function( inc ) {
			if ( inc === 1 && $(this).text() !== undefined ){
				//console.log(  $(this).text().trim() );
				directorNames += $(this).text().trim() +',';
			}
			inc++;
		});
	});

	   object['directors'] = directorNames.substring( 0, directorNames.length -1 );
return object;
}

function jsonArray( data, $ ){
let object = {};
$( data ).children("tr").map(function() {
	let itArr = [];
	x = $(this).children();
	x.each(function( inc ) {
		if( $(this).text() ){
			let tableValue = $(this).text().replace(/\s+/g, '_').toLowerCase();
			if ( inc === 0 ) {
				itArr.push( tableValue );
			} else {
				itArr.push( $(this).text().replace("₹", "") );
			}
		}
		inc++;
	});
	object[ itArr[0] ]  =  itArr[1]
}).get();
return object;
}

function getJsonFromDiv( data, $ ){
let object = {};
$( data ).map(function() {
	x = $(this).children();
	x.each(function( inc ) {
		let contactDetailDataArray = $(this).text().split(':');
		if( inc === 0 || inc === 1 ){
			if( contactDetailDataArray[1] !== undefined ){
				let key = contactDetailDataArray[0].replace(/\s+/g, '_').toLowerCase();
				object[key] = contactDetailDataArray[1]
			}
		} else if( inc === 3 ){
			object['address'] = contactDetailDataArray[0]
		} else {
			//console.log('No data found.')
		}
		inc++;
	});
});
return object;
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
let options = {uri: url, transform: function (body) {
	return cheerio.load(body);
}
};

return rp(options).then(function ($) {
	let stateArray 	= [];
	$('.data-extension.csv').filter( function( item,index ){
		let data 				= 	$(this),
			local				=	data['0'].attribs.href.split('data')[2].split('-'),
			fullURL				=	BASE_URL+data['0'].attribs.href,
			index1 				= 	fullURL.indexOf('-data-'),
			index2 				= 	fullURL.indexOf('-upto-'),
			finalStateName 		= 	fullURL.substring(index1 + 6,index2),
			arrStateName 		= 	finalStateName.split('-'),
			arrStateNameLength 	= 	arrStateName.length,
			stateNameWithConcat = 	'';

		for ( let j = 0; j <= arrStateNameLength; j++ ) {
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
let pageURL = '/catalog/company-master-data?title=&file_short_format=&page=' + pageNo;
return BASE_URL + pageURL;
}

function getFinalDataUrl(urnID) {
let localURL = 'https://www.zaubacorp.com/company/-/' + urnID;
request(localURL, function (error, response, html) {
	if (!error) {
		$('.company-data.uppercase').filter(function (item, index) {
			let data = $(this);
		});
	}
});
}

function getUrlToDownloadCSV(stateName) {
return BASE_URL + 'sites/default/files/dataurl15092015/company_master_data_upto_Mar_2015_' + stateName + ".csv";
}

function createTableByStateName( stateName ){
const stateTableName = sequelize.define(stateName, {
	cin 								: Sequelize.STRING,
	company_name						: Sequelize.STRING,
	company_status						: Sequelize.STRING,
	roc									: Sequelize.STRING,
	registration_number					: Sequelize.STRING,
	company_category					: Sequelize.STRING,
	company_sub_category				: Sequelize.STRING,
	class_of_company					: Sequelize.STRING,
	date_of_incorporation				: Sequelize.STRING,
	age_of_company						: Sequelize.STRING,
	activity							: Sequelize.STRING,
	number_of_members					: Sequelize.STRING,
	authorised_capital					: Sequelize.STRING,
	paid_up_capital						: Sequelize.STRING,
	number_of_employees					: Sequelize.STRING,
	listing_status						: Sequelize.STRING,
	date_of_last_annual_general_meeting	: Sequelize.STRING,
	date_of_latest_balance_sheet		: Sequelize.STRING,
	_email_id							: Sequelize.STRING,
	address								: Sequelize.STRING,
	directors							: Sequelize.STRING
});
return stateTableName;
}

app.listen('8081')

console.log('Magic happens on port 8081');

exports = module.exports = app;