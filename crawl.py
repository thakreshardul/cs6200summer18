import constants
import threading
import re
import json
import urllib.request
from urlelement import UrlElement
from frontier import Frontier
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from time import sleep, time

robot_dict = dict()
prev_domain = ''
frontier = Frontier()
lock = threading.Lock()
writer_lock = threading.Lock()


def is_allowed(url):
    scheme, netloc, path, params, query, fragment = urlparse(url)
    robot_url = scheme + "://" + netloc + "/robots.txt"
    try:
        if netloc not in robot_dict.keys():
            robot_parser = RobotFileParser(robot_url)
            robot_parser.read()
            robot_dict[netloc] = robot_parser
        robot_parser = robot_dict[netloc]
        return robot_parser.can_fetch("*", url)
    except:
        return True


def hit_url(url):
    # global prev_domain
    # if prev_domain in url:
    #     sleep(0.1)
    try:
        with urllib.request.urlopen(url, timeout=constants.URL_TIMEOUT) as response:
            # prev_domain = urlparse(url)[1]
            html_data = response.read()
            header = response.info()
            soup = BeautifulSoup(html_data, constants.HTML_PARSER)
            flag = True
            if soup.html.get('lang'):
                if 'en' not in soup.html['lang']:
                    flag = False
            if flag and 'text/html' in header['Content-Type']:
                return html_data, header
            else:
                raise LookupError
    except:
        return None, None


def filter_url(url):
    if url != "" and 'facebook' not in url and 'jpg' not in url and 'youtube' not in url and 'pdf' not in url \
            and 'twitter' not in url:
        for keyword in constants.KEYWORDS:
            if keyword in url.lower():
                return True
    return False


def canonicalize(url, base):
    # print(url)
    scheme, netloc, path, params, query, fragments = urlparse(url)
    base_scheme, base_netloc, base_path, base_params, base_query, base_fragments = urlparse(base)

    if not scheme:
        scheme = base_scheme.lower()
    if not netloc:
        netloc = base_netloc
    netloc = netloc.replace(':80', '').replace(':443', '').lower()
    new_url = urljoin(scheme + '://' + netloc, path.strip('/') + '/' + query, allow_fragments=False)
    if 'outurl' in new_url:
        scheme, netloc, path, params, query, fragments = urlparse(new_url.split('=')[1])
        new_url = urljoin(scheme + '://' + netloc, path.strip('/') + '/' + query, allow_fragments=False)

    if not filter_url(new_url):
        return ""
    try:
        new_url = urllib.request.urlopen(new_url, timeout=constants.URL_TIMEOUT).geturl().strip('/')
        return new_url
    except:
        return ""


def parse(raw_html, url):
    soup = BeautifulSoup(raw_html, constants.HTML_PARSER, from_encoding="iso-8859-1")
    if soup.title is not None:
        title = soup.title.string
    else:
        title = ''
    out_links = filter(lambda x: x != '',
                       map(lambda x: canonicalize(x['href'], url),
                           filter(lambda x: x.has_attr('href'), soup.find_all('a'))))
    [s.extract() for s in soup('script')]
    [s.extract() for s in soup('style')]
    text = soup.get_text().strip()
    text = re.sub('<[^>]+>', '', text, re.DOTALL).replace('\n', ' ')
    return title, text, out_links


def dump_to_file(raw_html, header, title, body, url_element, out_links, counter):
    # print("Dumping to file")
    with open("test/file{}".format(counter), 'w') as output_file:
        data = "<DOC>\n<DOCNO>{:s}</DOCNO>\n<HTTPHeader>{:s}</HTTPHeader>\n<TITLE>{:s}</TITLE>\n<DEPTH>{:d}</DEPTH>\n" \
               "<OUTLINKS>{:s}</OUTLINKS>\n<TEXT>{:s}</TEXT>\n<SOURCE>{:s}</SOURCE>\n</DOC>" \
            .format(url_element.url, str(header), title, url_element.depth, '\n'.join(out_links), body, str(raw_html))
        output_file.write(data)


def add_to_frontier(out_links, url_element):
    for link in out_links:
        lock.acquire()
        frontier.insert(UrlElement(link, 0, time(), depth=url_element.depth + 1), url_element.url)
        lock.release()


def crawl():
    crawled_url_count = 0
    writer_threads = list()
    visited = set()
    curr_t = time()

    while crawled_url_count < constants.TOTAL_DOCUMENTS:
        try:
            lock.acquire()
            url_element = frontier.pop()
            lock.release()
        except IndexError as e:
            print(e, frontier.size())
            lock.release()
            sleep(10)
            continue

        url = url_element.url
        if url not in visited:
            if is_allowed(url):
                raw_html, header = hit_url(url)
                if raw_html is not None and header is not None:
                    title, text, out_links = parse(raw_html, url)
                    t = threading.Thread(target=dump_to_file,
                                         args=(
                                             raw_html, header, title, text, url_element, out_links, crawled_url_count))
                    writer_threads.append(t)
                    t.start()
                    s = threading.Thread(target=add_to_frontier, args=(out_links, url_element))
                    writer_threads.append(s)
                    s.start()
                    visited.add(url)
                    if crawled_url_count % 50 == 0:
                        print("Crawled: {:d}\tTime_taken: {:f}\t Frontier Size: {:d}"
                              .format(crawled_url_count, time() - curr_t, frontier.size()))

                    crawled_url_count += 1

    for t in writer_threads:
        t.join()


def main():
    for url in constants.SEEDS:
        frontier.insert(UrlElement(url, constants.INITIAL_PRIORITY, time()), parent="")
    # frontier.print_frontier()
    print("Crawler started...")
    crawl()
    with open("in_links", 'w') as in_links:
        json.dump(frontier.in_links, in_links)


if __name__ == '__main__':
    start_time = time()
    main()
    end_time = time()
    print("Total time taken: {}".format(end_time - start_time))
