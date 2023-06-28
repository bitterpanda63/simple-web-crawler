import dotenv from 'dotenv';
dotenv.config()

import pg from 'pg';
const { Pool } = pg;

const client = new Pool();

import axios from 'axios';

import _jsdom from 'jsdom';
const { JSDOM } = _jsdom;

import Url from 'url-parse';
import normalize from 'normalize-url';

const parseLink = (link, base, prev_links) => {
    let url = new Url(link, base, false);
    try {
        let normalized_url = normalize(url.href, {
            defaultProtocol: 'https:',
            forceHttps: true,
            stripHash: true,
            removeQueryParameters: true
        });

        if (normalized_url == normalize(base))   {return null}
        if (prev_links.includes(normalized_url)) {return null}
        if(normalized_url.endsWith('.txt') || normalized_url.endsWith('.md') || normalized_url.endsWith('.png') || normalized_url.endsWith('.jpg') || normalized_url.endsWith('.pdf') || normalized_url.endsWith('.js') || normalized_url.endsWith('.css' || normalized_url.includes('web.archive.org/web'))) {
            return null
        }

        return normalized_url;
    }
    catch {
        return null
    }
}

const parseAnchors = (anchors, url) => {
    let links = [];
    anchors.forEach(_link => {
        let link = parseLink(_link.href, url, links);
        if(link) {links.push(link)};
    });

    return links
}

const indexUrl = url => {
    console.debug("Currently indexing ", url);
    let time = Date.now();
    axios.default.head(url, {timeout: 5000})
        .then(res => {
            let type = res.headers['content-type'];
            if(!type.startsWith('text/html') && !type.startsWith('application/xhtml')) { throw new Error('Not HTML Page') }
        })
        .then(() => axios.default.get(url, {timeout: 5000}))
        .then(res => new JSDOM(res.data))
        .then(async dom => {
            let _anchors = dom.window.document.querySelectorAll("a");
            let title = dom.window.document.title;
            let desc = dom.window.document.querySelector("meta[name=description]")?.content;
            let content = dom.window.document.body.textContent.replace(/[\n\r\s\t]+/g, ' ')
            let anchors = parseAnchors(_anchors, url);

            await client.query("UPDATE index SET title = $1, description = $2, content = $3, status = 'indexed', indexed_at = NOW() WHERE url = $4", [title, desc, content, url]);

            await client.query("INSERT INTO index(url) select * from unnest($1::text[]) on conflict (url) do nothing;", [anchors]);
            client.query("UPDATE index SET citations = citations + 1 WHERE url = ANY($1) ", [anchors]);
            console.log("Time Indexing " + url + " : " + (Date.now() - time)/1000)

            main();
        })
        .catch(async err => {
            console.log("Failed");
            await client.query("UPDATE index SET status = 'failed' WHERE url = $1", [url]);
            main();
        })
}

const main = async () => {
    let {rows:url} = await client.query("WITH cte AS ( SELECT url FROM index WHERE status = 'initial' ORDER BY citations DESC LIMIT 1 ) UPDATE index t SET status = 'processing' FROM cte WHERE cte.url = t.url RETURNING t.url");
    url = url[0].url;

    indexUrl(url);
};

const exit_function = () => {
    console.log('Exiting..')
}
//process.on('SIGINT', exit_function);
//process.on('exit', exit_function);

main();
