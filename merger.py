import constants
import glob
import json
import re
from time import time
from elasticsearch import Elasticsearch


def connect(host, port):
    """Connects to elastic search and returns the connection object."""
    return Elasticsearch([{'host': host, 'port': port}])


def parse_file(filename):
    doc_regex = re.compile('<DOCNO>.*?</DOCNO>', re.DOTALL)
    source_regex = re.compile('<SOURCE>.*?</SOURCE>', re.DOTALL)
    header_regex = re.compile('<HTTPHeader>.*?</HTTPHeader>', re.DOTALL)
    title_regex = re.compile('<TITLE>.*?</TITLE>', re.DOTALL)
    text_regex = re.compile('<TEXT>.*?</TEXT>', re.DOTALL)
    outlinks_regex = re.compile('<OUTLINKS>.*?</OUTLINKS>', re.DOTALL)
    depth_regex = re.compile('<DEPTH>.*?</DEPTH>', re.DOTALL)

    with open(filename, 'r') as datafile:
        # print(filename)
        try:
            filedata = datafile.read()
            docno = ''.join(re.findall(doc_regex, filedata)).replace('<DOCNO>', '').replace('</DOCNO>', '')
            header = ''.join(re.findall(header_regex, filedata)).replace('<HTTPHeader>', '').replace('</HTTPHeader>', '')
            title = ''.join(re.findall(title_regex, filedata)).replace('<TITLE>', '').replace('</TITLE>', '')
            text = ' '.join(''.join(re.findall(text_regex, filedata)).replace('<TEXT>', '').replace('</TEXT>', '').split())
            html_source = ''.join(re.findall(source_regex, filedata)).replace('<SOURCE>', '').replace('</SOURCE>', '')
            out_links = '\n'.join(re.findall(outlinks_regex, filedata)).replace('<OUTLINKS>', '').replace('</OUTLINKS>','')
            depth = ''.join(re.findall(depth_regex, filedata)).replace('<DEPTH>', '').replace('</DEPTH>', '')
            url = str(docno)
            return docno, header, title, text, html_source, out_links, depth, url
        except Exception as e:
            print(e)
            return None


def upload_to_index(elastic_client, docno, header, title, text, html_source, in_links, out_links, depth, url):
    try:
        res = elastic_client.get(index=constants.INDEX, doc_type=constants.DOC_TYPE, id=url)
        inl = res['_source']['in_links'].split('\n')
        in_links.extend(inl)
        current_author = set(map(lambda x: x.strip(), res['_source']['author'].split(';')))
        current_author.add('SHARDUL')
        body = {
            'doc': {
                'in_links': '\n'.join(set(in_links)),
                'author': '; '.join(current_author)
            }
        }
        elastic_client.update(index=constants.INDEX, doc_type=constants.DOC_TYPE, id=url, body=body)
    except Exception:
        body = dict(docno=docno, HTTPheader=header, title=title, text=text, html_Source=html_source,
                    in_links='\n'.join(in_links), out_links=out_links, author='SHARDUL', depth=depth, url=url)
        elastic_client.index(index=constants.INDEX, doc_type=constants.DOC_TYPE, id=url, body=body)


def main():
    es = connect(constants.HOST, constants.PORT)
    dataset = glob.glob(constants.DATASET_PATH)

    with open(constants.IN_LINKS, 'r') as input_file:
        in_links_dict = json.load(input_file)

    print(len(in_links_dict))
    count = 0
    for datafile in dataset:
        try:
            docno, header, title, text, html_source, out_links, depth, url = parse_file(datafile)

            if docno is None:
                raise FileNotFoundError
            if docno in in_links_dict.keys():
                in_links = in_links_dict[docno]
            else:
                in_links = list()
            upload_to_index(es, docno, header, title, text, html_source, in_links, out_links, depth, url)
            print('Completed {:d} {:s} Indexing'.format(count, docno))
            count += 1
        except Exception as e:
            print(e)
            print("Failed on file {:s}".format(datafile))

    print("Number of files indexed: {:d}".format(count))


if __name__ == "__main__":
    start_time = time()
    main()
    end_time = time()
    print('Total Time taken: {:f}'.format(end_time - start_time))
