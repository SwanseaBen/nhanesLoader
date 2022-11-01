import bisect
import os
import random
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_url_base(url):
    parsed_uri = urlparse(url)
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
    return result


def augment_url_with_site(url, site, pref="http"):
    if pref not in url:
        url = get_url_base(site) + ("/" if url[0] != "/" else "") + url
    return url


def get_links(url, extensions):
    r = requests.get(url)
    contents = r.content

    soup = BeautifulSoup(contents, "lxml")
    links = []
    for link in soup.findAll('a'):
        try:
            for extension in extensions:
                if extension in link['href']:
                    links.append(link['href'])
        except KeyError:
            pass
    return links


def remove_prefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def list_links(url, extension=[""]):
    for x in get_links(url, extension):
        print(x)


def go_through_directory(pathRemoval, link, outputDir):
    link = link.remove_prefix(pathRemoval)
    # while "\\" in link:
    # os.makedirs(path, exist_ok=True)


def download_links(links, pathRemoval, outputDir):
    cpt = 1
    for link in links:
        link2 = remove_prefix(link, pathRemoval)
        dir = os.path.dirname(link2)
        newDir = outputDir + "\\" + dir
        fname = newDir + "\\" + os.path.basename(link2)
        print(link)
        print("file ", cpt, " / ", len(links), " (" + fname + ")")
        cpt = cpt + 1
        try:
            os.makedirs(newDir, exist_ok=True)
            if not os.path.isfile(fname):
                response = requests.get(link, stream=True)
                with open(fname, "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                    handle.close()
            else:
                print("Skipped file as already created")
        except:
            print("!!! PROBLEM Creating ", fname)


def download_url_links(url, extensions, pathRemoval, outputDir):
    links = get_links(url, extensions)
    links = [augment_url_with_site(x, url) for x in links]
    download_links(links, pathRemoval, outputDir)


def download_nhanes(comp, year, type=1):
    #  prefix="https://wwwn.cdc.gov"
    for y in year:
        for c in comp:
            print()
            print(
                "======================================================================================================================")
            if (type == 1):
                url = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=" + c + "&CycleBeginYear=" + y
                removal = "https://wwwn.cdc.gov/Nchs/"
            else:
                url = "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + c + ".aspx?BeginYear=" + y
                removal = "https://wwwn.cdc.gov/nchs/data/"
            print(y, ":", c, "       =>", url)
            print("")
            types = [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"];
            links = get_links(url, [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"])
            linksHtm = get_links(url, [".htm"])
            for l in linksHtm:
                pre, ext = os.path.splitext(l)
                lXPT = pre + ".XPT"
                lDAT = pre + ".dat"
                lSAS = pre + ".sas"
                if (lXPT in links) or (lDAT in links) or (lSAS in links):
                    links.append(l)
            # links = [prefix + sub for sub in links]
            links = [augment_url_with_site(x, url) for x in links]
            random.shuffle(links)
            download_links(links, removal, "C:\Tmp\\")


def download_nhanes_b(comp, year):
    for y in year:
        for c in comp:
            download_links("https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + c + ".aspx?BeginYear=" + y,
                           [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"], "https://wwwn.cdc.gov/Nchs/data/",
                           "C:\Tmp\\")


def download_all_nhanes():
    # downloadNhanes(["Demographics"],["2017"]);
    # listLinks("https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=Demographics&CycleBeginYear=2017")

    download_nhanes(["Demographics", "Dietary", "Examination", "Laboratory", "Questionnaire", "Non-Public"],
                    ["2017", "2015", "2013", "2011", "2009", "2007", "2005", "2003", "2001", "1999"])
    download_nhanes(["Questionnaires", "labmethods", "Manuals", "Documents", "overview", "releasenotes", "overviewlab",
                     "overviewquex", "overviewexam"],
                    ["2017", "2015", "2013", "2011", "2009", "2007", "2005", "2003", "2001", "1999"], type=2)

    # downloadNhanes(["Demographics","Dietary","Examination","Questionnaire","Non-Public"],["1999"]);
    # downloadNhanesB(["Questionnaires","LabMethods","Manuals","Documents","DocContents","OverviewLab","OverviewQuex","OverviewExam"],["1999"]);
    # downloadLinks("https://wwwn.cdc.gov/nchs/nhanes/nhanes3/DataFiles.aspx", [".xpt",".dat",".sas",".txt",".pdf",".doc"], "https://wwwn.cdc.gov/nchs/data", "E:\Ben\Research\Datasets\Life Science\\")
    # downloadLinks("https://www.cdc.gov/nchs/nhanes/nh3data.htm", [".xpt",".dat",".sas",".txt",".pdf"], "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/nhanes","E:\Ben\Research\Datasets\Life Science\\")


def browse_directory_tables(dir, extensions=[""]):
    fNames = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            for ext in extensions:
                if ext in file:
                    fNames.append(os.path.join(root, file))
    return fNames


def count_elements(dir, attr=[""], all=False):
    seqn = []
    columns = []
    cpt = 0
    totalSize = 0

    notIncluded = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all:
                    for a in attr:
                        if a in file:
                            found = True
                if (not found) and (not all):
                    notIncluded.append(file)
                else:
                    fileName = os.path.join(root, file)
                    print('Opening file', fileName)
                    df = pd.read_sas(fileName)
                    if 'SEQN' in df:
                        totalSize = totalSize + os.path.getsize(fileName)
                        cpt = cpt + 1
                        for c in list(df):
                            columns.append(c)
                        for s in df['SEQN'].values:
                            seqn.append(s)
                    else:
                        notIncluded.append(file)
    print("========================= Not included: ====================================")
    print(notIncluded)
    columns = list(dict.fromkeys(columns))
    seqn = list(dict.fromkeys(seqn))
    columns.sort()
    seqn.sort()

    return seqn, columns, totalSize, cpt


def get_elements(seqn, columns, dir, attr, nbFiles=0, all=False):
    ls = len(seqn)
    lc = len(columns)
    data = np.empty((ls, lc))
    data[:] = np.NaN
    print("Loading Files")
    cpt = 0
    for root, dirs, files in os.walk(dir):
        for file in files:
            if ".XPT" in file:
                found = False
                if (not all):
                    for a in attr:
                        if a in file:
                            found = True
                if all or found:
                    fileName = os.path.join(root, file)
                    df = pd.read_sas(fileName)
                    allColumns = list(dict.fromkeys(list(df)))
                    if 'SEQN' in allColumns:
                        print('Reading file  ', cpt, "/", nbFiles, fileName)
                        cpt = cpt + 1
                        for index, row in df.iterrows():
                            sIndex = bisect.bisect_left(seqn, row['SEQN'])
                            for c in allColumns:
                                try:
                                    cIndex = bisect.bisect_left(columns, c)
                                    data[sIndex][cIndex] = row[c]
                                except ValueError:
                                    # print('Error:',row[c],type(row[c]), c, fileName)
                                    pass
    return data


def np_to_csv(data, columns, dest='e:/nhanesTestVeryFast3.csv'):
    header = ''
    for c in columns:
        header = header + c + ', '
    print("header")
    print(header)
    np.savetxt(dest, data, header=header, delimiter=', ', comments='')
    pass


def np_to_panda(data, columns):
    df = pd.DataFrame(data, columns=columns)
    return df


def nhanes_merger_numpy(dir, attr=[""], dest='e:/nhanesF.csv', all=False):
    seqn, columns, totalSize, nbFiles = count_elements(dir, attr, all)
    ls = len(seqn)
    lc = len(columns)
    print("===> Database filtering info:  ( nb Part", ls, ') (nb Columns', lc, ') (total file size (MBs)',
          totalSize / 1024 / 1024, ') (nb Files)', nbFiles)
    data = get_elements(seqn, columns, dir, attr, nbFiles, all)
    # npToCSV(data,columns,dest)
    df = np_to_panda(data, columns)
    df.to_csv(dest)
    return df


def load_csv(name, ageMin=-1, ageMax=200):
    df = pd.read_csv(name, low_memory=False)
    if 'RIDAGEYR' in df:
        l = [x and y for x, y in zip((df['RIDAGEYR'] >= ageMin), (df['RIDAGEYR'] <= ageMax))]
        return df[l]
    else:
        return df


def keep_non_null(df, col):
    l = (~df[col].isnull())
    return df[l]


def keep_equal(df, col, val):
    l = (df[col] == val)
    return df[l]


def keep_different(df, col, val):
    l = (df[col] != val)
    return df[l]


def keep_greater_than(df, col, val):
    l = (df[col] > val)
    return df[l]


def keep_greater_equal(df, col, val):
    l = (df[col] >= val)
    return df[l]


def keep_lower_than(df, col, val):
    l = (df[col] < val)
    return df[l]


def keep_lower_equal(df, col, val):
    l = (df[col] <= val)
    return df[l]


def keep_columns(df, cols):
    return df[cols]
