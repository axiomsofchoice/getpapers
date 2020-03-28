var fs = require('fs')
var chalk = require('chalk')
var got = require('got')
var mkdirp = require('mkdirp')
var _ = require('lodash')
var ProgressBar = require('progress')
var urlDl = require('./download.js')
var requestretry = require('requestretry')
var glob = require('matched')
var vc = require('version_compare')
var config = require('./config.js')
var log = require('winston')

var parseString = require('xml2js').parseString

var RoyalSociety = function (opts) {
  var royalsoc = this
  this.baseurl = 'https://royalsocietypublishing.org/' +
                 'action/doSearch?'
  this.opts = opts || {}
  royalsoc.first = true
  royalsoc.hitlimit = royalsoc.opts.hitlimit ? royalsoc.opts.hitlimit : 0
  royalsoc.hitcount = 0
  royalsoc.residualhits = 0
  royalsoc.allresults = []
  royalsoc.nextCursorMark = '*' // we always get back the first page
  royalsoc.pagesize = '1000'
  royalsoc.unfillledPage = false
}

RoyalSociety.prototype.search = function (query) {
  var royalsoc = this

  var options = { }
  if (!royalsoc.opts.all) {
    options['openAccess'] = 18
  }
  royalsoc.queryurl = royalsoc.buildQuery(query, options)

  if (royalsoc.opts.restart) {
    fs.readFile('royalsoc_results.json', (err, data) => {
      if ((err) && (err.code === 'ENOENT')) {
        log.error('No existing download to restart')
        process.exit(1)
      } else if (err) {
        throw err
      } else {
        log.info('Restarting previous download')
        royalsoc.allresults = JSON.parse(data)
        royalsoc.addDlTasks()
      }
    })
  } else {
    royalsoc.pageQuery()
  }
}

RoyalSociety.prototype.pageQuery = function () {
  var royalsoc = this

  var thisQueryUrl = royalsoc.queryurl + ''

  log.debug(thisQueryUrl)

  var retryOnHTTPNetOrEuPMCFailure = function (err, response, body, options) {

    log.debug("Statue code:" + response.statusCode)
    log.debug("Cookie Header:" + response.headers['set-cookie'])

    log.debug("Found Cookie Absent:" + body.indexOf('string:Cookie Absent'))
    log.debug("Found result__count:" + body.indexOf('<span class="result__count">'))

    return requestretry.RetryStrategies.HTTPOrNetworkError(err, response, body) ||
            (-1 == body.indexOf('string:Cookie Absent')) ||
            (-1 == body.indexOf('<span class="result__count">')) // Not on a results page.
  }

  var rq = requestretry.get({url: thisQueryUrl,
    maxAttempts: 2, // TODO: Make this configurable?
    retryStrategy: retryOnHTTPNetOrEuPMCFailure,
    headers: {'User-Agent': config.userAgent},
    // Royal Society Publishing requires each request to provide cookies otherwise gives 302 status code.
    jar: true
  })
  var handleResquestResponse = function (data) {
    if (data.attempts > 1) {
      log.warn('We had to retry the last request ' + data.attempts + ' times.')
    }

    log.debug("Statue code:" + data.statusCode)

    // Find the number of results and the number that is displayed on this page.
    let resultCountRegex = /\<span class\=\"result__count\"\>(\d+)\<\/span\>/;
    let resultCount = data.body.indexOf('result__count')
    log.debug("indexOf result__count: " + resultCount)
    if (resultCount) {
        let foundResultCount = resultCountRegex.exec(data.body)
        log.debug("Total number of results: " + foundResultCount[1])
    }

    // Split up the results and recompose them as XML.
    var resultsAsArray = []
    var bodyWithoutNewlines = data.body.replace(/\n/g, " ")
    let myRe = /(\<\!\-\- XSLT.*?END OF XSLT \-\-\>)/g

    resultsAsArray = bodyWithoutNewlines.match(myRe)

    log.debug(resultsAsArray)
    let resultsAsXml = '<body>' + resultsAsArray.join('\n') + '</body>'

    // FIXME: For debug.
    log.debug(resultsAsXml)
    data.body = resultsAsXml

    convertXML2JSON(data)
  }
  var convertXML2JSON = function (data) {
    parseString(data.body, function (err, datum) {
      if (err) throw err
      var cb = royalsoc.completeCallback.bind(royalsoc, datum)
      cb()
    })
  }
  rq.then(handleResquestResponse)
  rq.on('timeout', royalsoc.timeoutCallback)
}

RoyalSociety.prototype.completeCallback = function (data) {
  var royalsoc = this

  var resp = data.responseWrapper

  if (!resp.hitCount || !resp.hitCount[0] || !resp.resultList[0].result) {
    log.error('Malformed or empty response from Royal Society Publishing. Try running again. Perhaps your query is wrong.')
    process.exit(1)
  }

  if (royalsoc.first) {
    royalsoc.first = false
    royalsoc.hitcount = parseInt(resp.hitCount[0])
    var oaclause = royalsoc.opts.all ? '' : ' open access'
    log.info('Found ' + royalsoc.hitcount + oaclause + ' results')

    royalsoc.testApi(resp.version[0])

    if (royalsoc.hitcount === 0 || royalsoc.opts.noexecute) {
      process.exit(0)
    }

    // set hitlimit
    if (royalsoc.hitlimit && royalsoc.hitlimit < royalsoc.hitcount) {
      log.info('Limiting to ' + royalsoc.hitlimit + ' hits')
    } else { royalsoc.hitlimit = royalsoc.hitcount }

    // create progress bar
    var progmsg = 'Retrieving results [:bar] :percent' +
                  ' (eta :etas)'
    var progopts = {
      total: royalsoc.hitlimit,
      width: 30,
      complete: chalk.green('=')
    }
    royalsoc.pageprogress = new ProgressBar(progmsg, progopts)
  }
  var result
  if (royalsoc.residualhits) {
    result = resp.resultList[0].result.slice(0, royalsoc.residualhits)
  } else {
    result = resp.resultList[0].result
    // if less results in this page than page count (and we were expecting an entire page)
    // EuPMC has been lying and we shouldn't keep searching for more results
    if (result.length < royalsoc.pagesize) royalsoc.unfilledPage = true
  }
  log.debug('In this batch got: ' + result.length + ' results')
  royalsoc.allresults = royalsoc.allresults.concat(result)
  royalsoc.pageprogress.tick(result.length)

  if (royalsoc.allresults.length < royalsoc.hitlimit) { // we still have more results to get
    if (royalsoc.unfilledPage) { // but the last page wasn't full then something is wrong
      log.info('EuPMC gave us the wrong hitcount. We\'ve already found all the results')
      royalsoc.handleSearchResults(royalsoc)
      return
    }
    if (royalsoc.hitlimit - royalsoc.allresults.length < royalsoc.pagesize) {
      royalsoc.residualhits = royalsoc.hitlimit - royalsoc.allresults.length
    }
    royalsoc.nextCursorMark = resp.nextCursorMark[0]
    royalsoc.pageQuery()
  } else {
    log.info('Done collecting results')
    royalsoc.handleSearchResults(royalsoc)
  }
}

RoyalSociety.prototype.timeoutCallback = function (ms) {
  var royalsoc = this
  log.error('Did not get a response from Royal Society Publishing within ' + ms + 'ms')
  if (royalsoc.allresults) {
    log.info('Handling the limited number of search results we got.')
    log.warn('The metadata download did not finish so you *will* be missing some results')
    royalsoc.handleSearchResults(royalsoc)
  }
}

RoyalSociety.prototype.buildQuery = function (query, options) {
  var royalsoc = this

  var queryurl = royalsoc.baseurl + 'text1=' + encodeURIComponent(query)
  Object.keys(options).forEach(function (key) {
    var val = options[key]
    if (key.length > 0) {
      queryurl += '&' + key + '=' + val
    }
  })
  return queryurl
}

RoyalSociety.prototype.formatResult = function (result) {
  return result.authorString +
  ' (' + result.pubYear + '). ' +
  result.title + ' https://doi.org/' + result.DOI
}

RoyalSociety.prototype.handleSearchResults = function (royalsoc) {
  // see how many results were unique
  var originalLength = royalsoc.allresults.length
  royalsoc.allresults = _.uniq(royalsoc.allresults, function (x) {
    return royalsoc.getIdentifier(x).id
  })
  if (royalsoc.allresults.length < originalLength) {
    log.info('Duplicate records found: ' +
             royalsoc.allresults.length +
             ' unique results identified')
  }

  if (royalsoc.allresults.length > royalsoc.hitlimit) {
    royalsoc.allresults = royalsoc.allresults.slice(0, royalsoc.hitlimit)
    log.info('limiting hits')
  }

  // write the full result set to a file
      log.info('Saving result metadata')
  var pretty = JSON.stringify(royalsoc.allresults, null, 2)
  fs.writeFileSync('royalsoc_results.json', pretty)
  var resultsFilename = chalk.blue('royalsoc_results.json')
  log.info('Full EUPMC result metadata written to ' + resultsFilename)

  // write individual results to their respective directories
  royalsoc.allresults.forEach(function (result) {
    royalsoc.writeRecord(result, royalsoc)
  })
  log.info('Individual EUPMC result metadata records written')

  // write only the url list to file
  log.info('Extracting fulltext HTML URL list (may not be available for all articles)')
  var urls = royalsoc.allresults
    .map(royalsoc.getFulltextHTMLUrl, royalsoc)
    .filter(function (x) { return !(x === null) })

  if (urls.length > 0) {
    fs.writeFileSync(
      'royalsoc_fulltext_html_urls.txt',
      urls.concat(['\n']).join('\n')
    )
    var urlFilename = chalk.blue('royalsoc_fulltext_html_urls.txt')
    log.info('Fulltext HTML URL list written to ' + urlFilename)
  }

  royalsoc.addDlTasks()
}

RoyalSociety.prototype.addDlTasks = function () {
  var royalsoc = this
  var dlTasks = []

  // download the fullText PDF
  if (royalsoc.opts.pdf) {
    dlTasks.push(royalsoc.downloadFulltextPDFs)
  }

  royalsoc.runDlTasks(dlTasks)
}

RoyalSociety.prototype.runDlTasks = function (dlTasks) {
  var royalsoc = this

  royalsoc.dlTasks = dlTasks
  royalsoc.currDlTask = -1
  royalsoc.nextDlTask()
}

RoyalSociety.prototype.nextDlTask = function () {
  var royalsoc = this

  royalsoc.currDlTask ++
  if (royalsoc.dlTasks.length > royalsoc.currDlTask) {
    var fun = royalsoc.dlTasks[royalsoc.currDlTask]
    fun(royalsoc)
  } else {
    process.exit(0)
  }
}

RoyalSociety.prototype.downloadFulltextPDFs = function (royalsoc) {
  var urls = royalsoc.allresults
    .map(royalsoc.getFulltextPDFUrl, royalsoc)
    .filter(function (x) { return !(x === null) })

  log.info('Downloading fulltext PDF files')

  var urlQueue = royalsoc.urlQueueBuilder(urls, 'PDF', 'fulltext.pdf')
  urlDl.downloadurlQueue(urlQueue, royalsoc.nextDlTask.bind(royalsoc))
}

RoyalSociety.prototype.downloadUrls = function (urls, type, rename, failed,
  cb, thisArg, fourohfour) {
  // setup progress bar
  var progmsg = 'Downloading files [:bar] :percent' +
                ' (:current/:total) [:elapseds elapsed, eta :eta]'
  var progopts = {
    total: urls.length,
    width: 30,
    complete: chalk.green('=')
  }
  var dlprogress = new ProgressBar(progmsg, progopts)

  urls.forEach(function (urlId) {
    var url = urlId[0]
    var id = urlId[1]
    var base = id + '/'
    log.debug('Creating directory: ' + base)
    mkdirp.sync(base)
    log.debug('Downloading ' + type + ': ' + url)
    var options = {
      timeout: 15000,
      encoding: null
    }
    got(url, options, function (err, data, res) {
      dlprogress.tick()
      if (err) {
        if (err.code === 'ETIMEDOUT' || err.code === 'ESOCKETTIMEDOUT') {
          log.warn('Download timed out for URL ' + url)
        }
        if (!res) {
          failed.push(url)
        } else if ((res.statusCode === 404) && !(fourohfour === null)) {
          fourohfour()
        } else {
          failed.push(url)
        }
        cb()
      } else {
        fs.writeFile(base + rename, data, cb)
      }
    })
  })
}

RoyalSociety.prototype.getFulltextHTMLUrl = function (result, oa) {
  var royalsoc = this
  var id = royalsoc.getIdentifier(result)

  if (!result.fullTextUrlList) { return royalsoc.noFulltextUrls(id) }

  var urls = result.fullTextUrlList[0].fullTextUrl
  var htmlUrls = urls.filter(function (u) {
    return (u.documentStyle[0] === 'html' || u.documentStyle[0] === 'doi')
  }).sort(function (a, b) {
    return (a.availabilityCode[0] === 'OA' || royalsoc.opts.all) ? -1 : 1
  })
  if (htmlUrls.length === 0) {
    log.warn('Article with ' + id.type + ' "' +
             id.id + '" had no fulltext HTML url')
    return null
  } else {
    return htmlUrls[0].url[0]
  }
}

RoyalSociety.prototype.getIdentifier = function (result) {
  var types = ['pmcid', 'doi', 'pmid', 'title']
  for (var i = 0; i < types.length; i++) {
    var type = types[i]
    if (result.hasOwnProperty(type) && result[type].length > 0) {
      return {
        type: type,
        id: result[type][0]
      }
    }
  }

  return {
    type: 'error',
    id: 'unknown ID'
  }
}

RoyalSociety.prototype.getFulltextXMLUrl = function (result) {
  var royalsoc = this

  var id = royalsoc.getIdentifier(result)

  var xmlurl = null

  if (id.type === 'pmcid') {
    xmlurl = 'https://www.ebi.ac.uk/europepmc/webservices/rest/' +
    id.id + '/fullTextXML'
  } else {
    log.warn('Article with ' + id.type + ' "' +
             id.id + ' did not have a PMCID (therefore no XML)')
    return null
  }

  if (!result.fullTextUrlList) { return royalsoc.noFulltextUrls(id) }

  var urls = result.fullTextUrlList[0].fullTextUrl
  var htmlUrls = urls.filter(function (u) {
    return (u.documentStyle[0] === 'html' || u.documentStyle[0] === 'doi')
  }).filter(function (a, b) {
    return (a.availabilityCode[0] === 'OA')
  })
  if (htmlUrls.length === 0) {
    log.warn('Article with ' + id.type + ' "' +
             id.id + '" was not Open Access (therefore no XML)')
    return null
  }

  return [xmlurl, id.id]
}

RoyalSociety.prototype.getFulltextPDFUrl = function (result) {
  var royalsoc = this
  var id = royalsoc.getIdentifier(result)

  var noPDF = function (id) {
    log.warn('Article with ' + id.type + ' "' +
                 id.id + '" had no fulltext PDF url')
    return null
  }

  if (!result.fullTextUrlList) { return royalsoc.noFulltextUrls(id) }
  if (result.hasPDF === 'N') { return noPDF(id) }

  var urls = result.fullTextUrlList[0].fullTextUrl
  var pdfOAurls = urls.filter(function (u) {
    return u.documentStyle[0] === 'pdf' &&
    u.availabilityCode[0] === 'OA'
  })

  if (pdfOAurls.length === 0) {
    return noPDF(id)
  } else {
    return [pdfOAurls[0].url[0], id.id]
  }
}

RoyalSociety.prototype.urlQueueBuilder = function (urls, type, rename) {
  return urls.map(function (urlId) {
    return { url: urlId[0], id: urlId[1], type: type, rename: rename }
  })
}

RoyalSociety.prototype.writeRecord = function (record, royalsoc) {
  var json = JSON.stringify(record, null, 2)
  var id = royalsoc.getIdentifier(record).id
  mkdirp.sync(id)
  fs.writeFileSync(id + '/royalsoc_result.json', json)
}

RoyalSociety.prototype.noFulltextUrls = function (id) {
  log.debug('Article with ' + id.type + ' "' +
           id.id + '" had no fulltext Urls')
  return null
}

module.exports = RoyalSociety
