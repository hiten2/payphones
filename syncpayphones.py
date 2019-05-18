import csv
import os
import random
import re
import sys
import time
import urllib2

__doc__ = "gross script to extract US payphone data"

global STATE_ABBREVIATIONS

with open("usstates.csv", "rb") as fp:
    reader = csv.reader(fp)
    reader.next() # skip header
    STATE_ABBREVIATIONS = [row[2] for row in reader]

global USER_AGENTS

with open("user-agents.txt", "rb") as fp:
    reader = csv.reader(fp)
    USER_AGENTS = ["".join(row) for row in reader]

def extract_payphones(html):
    """
    generate payphone dictionaries from HTML

    the HTML follows a rigid format, so a messy solution is simpler
    """
    pattern = "<tr><td class=\"address_highlight\">" \
        "<a href=\".*\">\n" \
        "<font color=\".*\"><b>.*</b></font></a> </td>" \
        "<td>.*</td><td>.*<br></td></tr>"
    
    for match in re.findall(pattern, html):
        start = match.find("<a href=\"") + len("<a href=\"")
        stop = match.find("\">\n", start)
        payphone = {"href": match[start:stop]}
        
        start = match.find("<b>", stop) + len("<b>")
        stop = match.find("</b>", start)
        payphone["number"] = '-'.join(filter(None,
            match[start:stop]
                .replace('\'', "").replace('"', "")
                .replace('(', "").replace(')', "")
                .replace('-', ' ').split(' ')))

        start = match.find(" </td><td>", stop) + len(" </td><td>")
        stop = match.find("</td><td>", start)
        payphone["name"] = match[start:stop].replace('\'', "").replace('"', "")

        start = match.find("</td><td>", stop) + len("</td><td>")
        stop = match.find("<br>", start)
        payphone["address"] = match[start:stop].replace('\'', "").replace('"', "")

        yield {k: urllib2.unquote(v) for k, v in payphone.iteritems()}

def extract_towns(html):
    """generate towns from HTML"""
    for match in re.findall("<a href=\"/numbers/usa/.*/.*\"", html):
        yield match[match.find('/', 23):match.rfind('/')].strip('/')

def sync():
    """
    synchronize the CSV file with the remote database

    this uses varied user-agents, and on error it sleeps for
    a random interval (up to, but not including 1 second)
    """
    request_factory = lambda u: urllib2.Request(u, headers = {"User-Agent":
        random.choice(USER_AGENTS)})
    url_root = "http://www.payphone-project.com/numbers/usa/"
    
    with open("uspayphones.csv", "wb") as fp:
        writer = csv.writer(fp)
        writer.writerow(["address", "name", "number", "state_abbreviation", "town"])

        for abbreviation in STATE_ABBREVIATIONS:
            state_url = os.path.join(url_root, abbreviation)
            print "[*] Operating in state", abbreviation, "from", state_url
            state_request = request_factory(state_url)
            
            for town in extract_towns(urllib2.urlopen(state_request
                    ).read()):
                town_url = os.path.join(state_url, town)
                print "\t[*] Scanning", town, "from", town_url
                
                try:
                    while 1:
                        try:
                            town_request = request_factory(town_url)
                            
                            for payphone in extract_payphones(urllib2.urlopen(
                                    town_request).read()):
                                print "\t\t[*] Found", payphone["name"],
                                print "(%s)" % payphone["number"],
                                print "at", payphone["address"], "in",
                                print town.replace('_', ' ')
                                writer.writerow([payphone["address"],
                                    payphone["name"], payphone["number"],
                                    abbreviation, town.replace('_', ' ')])
                                os.fdatasync(fp.fileno())
                            break
                        except (urllib2.HTTPError, urllib2.URLError) as e:
                            print >> sys.stderr, "\x1b[31m[!] Failed" \
                                " to GET %s:" % town_url, e, "\x1b[39m"
                            time.sleep(random.random())
                except KeyboardInterrupt:
                    print "[*] Quitting GET attempts"
                    return # break out of everything

if __name__ == "__main__":
    sync()
