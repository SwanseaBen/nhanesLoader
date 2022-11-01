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


def augment_url_with_site(url, site, prefix="http"):
    if prefix not in url:
        url = get_url_base(site) + ("/" if url[0] != "/" else "") + url
    return url


def get_links(url, extensions):
    request = requests.get(url)
    contents = request.content

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


def remove_prefix(path, prefix):
    if path.startswith(prefix):
        return path[len(prefix):]
    return path


def list_links(url, extensions=[""]):
    for link in get_links(url, extensions):
        print(link)


def go_through_directory(path, link, output_dir):
    link = link.remove_prefix(path)
    # while "\\" in link:
    # os.makedirs(path, exist_ok=True)


def download_links(links, path, output_dir):
    count = 1
    for link in links:
        link_processed = remove_prefix(link, path)
        nhanes_dir = os.path.dirname(link_processed)
        new_directory = output_dir + "\\" + nhanes_dir
        file_name = new_directory + "\\" + os.path.basename(link_processed)
        print(link)
        print("file ", count, " / ", len(links), " (" + file_name + ")")
        count = count + 1
        try:
            os.makedirs(new_directory, exist_ok=True)
            if not os.path.isfile(file_name):
                response = requests.get(link, stream=True)
                with open(file_name, "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                    handle.close()
            else:
                print("Skipped file as already created")
        except:
            print("!!! PROBLEM Creating ", file_name)


def download_url_links(url, extensions, path, output_dir):
    links = get_links(url, extensions)
    links = [augment_url_with_site(link, url) for link in links]
    download_links(links, path, output_dir)


def download_nhanes(components, years, type=1):
    #  prefix="https://wwwn.cdc.gov"
    for year in years:
        for component in components:
            print()
            print(
                "======================================================================================================================")
            if (type == 1):
                url = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=" + component + "&CycleBeginYear=" + year
                removal = "https://wwwn.cdc.gov/Nchs/"
            else:
                url = "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + component + ".aspx?BeginYear=" + year
                removal = "https://wwwn.cdc.gov/nchs/data/"
            print(year, ":", component, "       =>", url)
            print("")
            types = [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"]
            links = get_links(url, [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"])
            links_htm = get_links(url, [".htm"])
            for link in links_htm:
                pre, ext = os.path.splitext(link)
                links_xpt = pre + ".XPT"
                links_dat = pre + ".dat"
                links_sas = pre + ".sas"
                if (links_xpt in links) or (links_dat in links) or (links_sas in links):
                    links.append(link)
            # links = [prefix + sub for sub in links]
            links = [augment_url_with_site(x, url) for x in links]
            random.shuffle(links)
            download_links(links, removal, "C:\Tmp\\")


def download_nhanes_b(components, years):
    for year in years:
        for component in components:
            download_links("https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + component + ".aspx?BeginYear=" + year,
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


def browse_directory_tables(directory, extensions=[""]):
    file_names = []
    for root, directories, files in os.walk(directory):
        for file in files:
            for extension in extensions:
                if extension in file:
                    file_names.append(os.path.join(root, file))
    return file_names


def count_elements(directory, attributes=[""], all=False):
    sequence_numbers = []
    columns = []
    count = 0
    total_size = 0

    not_included = []
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all:
                    for attribute in attributes:
                        if attribute in file:
                            found = True
                if (not found) and (not all):
                    not_included.append(file)
                else:
                    file_name = os.path.join(root, file)
                    print('Opening file', file_name)
                    data = pd.read_sas(file_name)
                    if 'SEQN' in data:
                        total_size = total_size + os.path.getsize(file_name)
                        count = count + 1
                        for column in list(data):
                            columns.append(column)
                        for sequence_number in data['SEQN'].values:
                            sequence_numbers.append(sequence_number)
                    else:
                        not_included.append(file)
    print("========================= Not included: ====================================")
    print(not_included)
    columns = list(dict.fromkeys(columns))
    sequence_numbers = list(dict.fromkeys(sequence_numbers))
    columns.sort()
    sequence_numbers.sort()

    return sequence_numbers, columns, total_size, count


def get_elements(sequence_numbers, columns, directory, attributes, num_files=0, all=False):
    total_sequences = len(sequence_numbers)
    total_columns = len(columns)
    data = np.empty((total_sequences, total_columns))
    data[:] = np.NaN
    print("Loading Files")
    count = 0
    for root, directories, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if (not all):
                    for a in attributes:
                        if a in file:
                            found = True
                if all or found:
                    file_name = os.path.join(root, file)
                    df = pd.read_sas(file_name)
                    columns = list(dict.fromkeys(list(df)))
                    if 'SEQN' in columns:
                        print('Reading file  ', count, "/", num_files, file_name)
                        count = count + 1
                        for index, row in df.iterrows():
                            sequence_index = bisect.bisect_left(sequence_numbers, row['SEQN'])
                            for column in columns:
                                try:
                                    column_index = bisect.bisect_left(columns, column)
                                    data[sequence_index][column_index] = row[column]
                                except ValueError:
                                    # print('Error:',row[column],type(row[column]), column, file_name)
                                    pass
    return data


def np_to_csv(data, columns, destination='e:/nhanesTestVeryFast3.csv'):
    header = ''
    for column in columns:
        header = header + column + ', '
    print("header")
    print(header)
    np.savetxt(destination, data, header=header, delimiter=', ', comments='')


def np_to_panda(data, columns):
    data = pd.DataFrame(data, columns=columns)
    return data


def nhanes_merger_numpy(directory, attributes=[""], destination='e:/nhanesF.csv', all=False):
    sequence_numbers, columns, total_size, num_files = count_elements(directory, attributes, all)
    total_sequences = len(sequence_numbers)
    total_columns = len(columns)
    print("===> Database filtering info:  ( nb Part", total_sequences, ') (nb Columns', total_columns,
          ') (total file size (MBs)',
          total_size / 1024 / 1024, ') (nb Files)', num_files)
    data = get_elements(sequence_numbers, columns, directory, attributes, num_files, all)
    # npToCSV(data, columns, destination)
    df = np_to_panda(data, columns)
    df.to_csv(destination)
    return df


def load_csv(name, min_age=-1, max_age=200):
    data = pd.read_csv(name, low_memory=False)
    if 'RIDAGEYR' in data:
        l = [x and y for x, y in zip((data['RIDAGEYR'] >= min_age), (data['RIDAGEYR'] <= max_age))]
        return data[l]
    else:
        return data


def keep_non_null(data, column):
    x = (~data[column].isnull())
    return data[x]


def keep_equal(data, column, value):
    x = (data[column] == value)
    return data[x]


def keep_different(data, column, value):
    x = (data[column] != value)
    return data[x]


def keep_greater_than(data, column, value):
    x = (data[column] > value)
    return data[x]


def keep_greater_equal(data, column, value):
    x = (data[column] >= value)
    return data[x]


def keep_lower_than(data, column, value):
    x = (data[column] < value)
    return data[x]


def keep_lower_equal(data, column, value):
    x = (data[column] <= value)
    return data[x]


def keep_columns(data, columns):
    return data[columns]
