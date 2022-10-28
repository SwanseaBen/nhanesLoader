from bs4 import BeautifulSoup
import sys
import requests
import os

import numbers
import math 
import bisect

from nhanesVariables import tests
import numpy as np
import random
import time
from tqdm import tqdm
from urllib.parse import urlparse
import pandas as pd




def getURLBase(url):
    parsed_uri = urlparse(url )
    result = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
    return result

def augmentURLWithSite(url,site,pref="http"):
    if pref not in url:
        url=getURLBase(site)+("/" if url[0]!="/" else "") + url
    return url

def get_links(url, extensions):

    r = requests.get(url)
    contents = r.content

    soup = BeautifulSoup(contents, "lxml")
    links =  []
    for link in soup.findAll('a'):
        try:
            for extension in extensions:
                if extension in link['href']:
                    links.append(link['href'])
        except KeyError:
            pass
    return links

def removePrefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s  

def listLinks(url, extension=[""]):
    for x in get_links(url,extension):
        print(x)

def goThroughDirectory(pathRemoval,link, outputDir):
    link=link.removePrefix(pathRemoval)
    #while "\\" in link:
    #os.makedirs(path, exist_ok=True)

def downloadLinks(links,pathRemoval, outputDir):
        cpt=1
        for link in links:
            link2=removePrefix(link,pathRemoval)
            dir=os.path.dirname(link2)
            newDir=outputDir+"\\"+dir
            fname=newDir+"\\"+os.path.basename(link2)
            print(link)
            print("file ",cpt," / ", len(links), " ("+fname+")")
            cpt=cpt+1
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
                print("!!! PROBLEM Creating ",fname)

def downloadURLLinks(url, extensions,pathRemoval, outputDir):
    links=get_links(url,extensions)
    links = [augmentURLWithSite(x,url) for x in links]
    downloadLinks(links,pathRemoval, outputDir)

def downloadNhanes(comp,year,type=1):
 #  prefix="https://wwwn.cdc.gov"
   for y in year:
       for c in comp:
           print()
           print("======================================================================================================================")
           if (type==1):
                url="https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component="+ c +"&CycleBeginYear="+y
                removal="https://wwwn.cdc.gov/Nchs/"
           else:
                url="https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/"+c+".aspx?BeginYear="+y
                removal="https://wwwn.cdc.gov/nchs/data/"
           print(y,":",c,"       =>",url)
           print("")
           types=[".XPT",".dat",".sas",".txt",".pdf",".doc"];
           links=get_links(url,[".XPT",".dat",".sas",".txt",".pdf",".doc"])
           linksHtm=get_links(url,[".htm"])
           for l in linksHtm:
               pre, ext = os.path.splitext(l)
               lXPT=pre+".XPT"
               lDAT=pre+".dat"
               lSAS=pre+".sas"
               if (lXPT in links) or (lDAT in links) or (lSAS in links):
                   links.append(l)
           #links = [prefix + sub for sub in links]
           links = [augmentURLWithSite(x,url) for x in links]
           random.shuffle(links)
           downloadLinks(links, removal, "C:\Tmp\\")

def downloadNhanesB(comp,year):
   for y in year:
       for c in comp:
           downloadLinks("https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/"+c+".aspx?BeginYear="+y, [".XPT",".dat",".sas",".txt",".pdf",".doc"], "https://wwwn.cdc.gov/Nchs/data/", "C:\Tmp\\")

def downloadAllNhanes():
    #downloadNhanes(["Demographics"],["2017"]);
    #listLinks("https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=Demographics&CycleBeginYear=2017")

    downloadNhanes(["Demographics","Dietary","Examination","Laboratory","Questionnaire","Non-Public"],["2017","2015","2013","2011","2009","2007","2005","2003","2001","1999"])
    downloadNhanes(["Questionnaires","labmethods","Manuals","Documents","overview","releasenotes","overviewlab","overviewquex","overviewexam"],["2017","2015","2013","2011","2009","2007","2005","2003","2001","1999"],type=2)

    #downloadNhanes(["Demographics","Dietary","Examination","Questionnaire","Non-Public"],["1999"]);
    #downloadNhanesB(["Questionnaires","LabMethods","Manuals","Documents","DocContents","OverviewLab","OverviewQuex","OverviewExam"],["1999"]);
    #downloadLinks("https://wwwn.cdc.gov/nchs/nhanes/nhanes3/DataFiles.aspx", [".xpt",".dat",".sas",".txt",".pdf",".doc"], "https://wwwn.cdc.gov/nchs/data", "E:\Ben\Research\Datasets\Life Science\\")
    #downloadLinks("https://www.cdc.gov/nchs/nhanes/nh3data.htm", [".xpt",".dat",".sas",".txt",".pdf"], "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/nhanes","E:\Ben\Research\Datasets\Life Science\\")
    

def BrowseDirectoryTables(dir,extensions=[""]):
    fNames=[]
    for root, dirs, files in os.walk(dir):
        for file in files:
            for ext in extensions:
                if ext in file:
                    fNames.append(os.path.join(root, file))
    return fNames



def countElements(dir,attr=[""], all=False):
    seqn=[]
    columns=[]
    cpt=0
    totalSize=0

    notIncluded=[]
    for root, dirs, files in os.walk(dir):
        for file in files:
            if ".XPT" in file:
                found=False
                if not all:
                    for a in attr:
                        if a in file:
                            found=True
                if (not found) and (not all):
                    notIncluded.append(file)
                else:
                    fileName=os.path.join(root,  file)
                    print('Opening file',fileName)
                    df = pd.read_sas(fileName)
                    if 'SEQN' in df:
                        totalSize=totalSize+os.path.getsize(fileName)
                        cpt=cpt+1
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

    return seqn,columns,totalSize,cpt

def getElements(seqn, columns, dir, attr, nbFiles=0, all=False):
    ls=len(seqn)
    lc=len(columns)
    data = np.empty((ls,lc))
    data[:]=np.NaN
    print("Loading Files")
    cpt=0
    for root, dirs, files in os.walk(dir):
        for file in files:
            if ".XPT" in file:
                found=False
                if (not all):
                    for a in attr:
                        if a in file:
                            found=True
                if all or found:
                    fileName=os.path.join(root,  file)
                    df = pd.read_sas(fileName)
                    allColumns = list(dict.fromkeys(list(df)))
                    if 'SEQN' in allColumns:
                        print('Reading file  ',cpt,"/",nbFiles, fileName)
                        cpt=cpt+1
                        for index, row in df.iterrows():
                            sIndex=bisect.bisect_left(seqn,row['SEQN'])
                            for c in allColumns:
                                try:
                                    cIndex=bisect.bisect_left(columns,c)
                                    data[sIndex][cIndex]=row[c]
                                except ValueError:
                                    #print('Error:',row[c],type(row[c]), c, fileName)
                                    pass
    return data
                        
def npToCSV(data,columns,dest='e:/nhanesTestVeryFast3.csv'):
    header=''
    for c in columns:
        header=header+c+', '
    print("header")
    print(header)
    np.savetxt(dest,data,header=header, delimiter=', ', comments='' )
    pass

def npToPanda(data,columns):
    df=pd.DataFrame(data,columns=columns)
    return df

def nhanesMergerNumpy(dir,attr=[""],dest='e:/nhanesF.csv',all=False):
    seqn,columns,totalSize,nbFiles=countElements(dir,attr,all)
    ls=len(seqn)
    lc=len(columns)
    print("===> Database filtering info:  ( nb Part",ls,') (nb Columns', lc,') (total file size (MBs)',totalSize/1024/1024,') (nb Files)',nbFiles)
    data = getElements(seqn,columns,dir,attr,nbFiles,all)
    #npToCSV(data,columns,dest)
    df= npToPanda(data,columns)
    df.to_csv(dest)
    return df

def loadCSV(name,ageMin=-1,ageMax=200):

    df=pd.read_csv(name, low_memory=False)
    if 'RIDAGEYR' in df:
        l=[x and y for x, y in zip((df['RIDAGEYR']>=ageMin),(df['RIDAGEYR']<=ageMax))]
        return df[l] 
    else:
       return df

def KeepNonNull(df,col):
    l=(~df[col].isnull())
    return df[l]

def KeepEqual(df,col,val):
    l=(df[col]==val)
    return df[l]

def KeepDifferent(df,col,val):
    l=(df[col]!=val)
    return df[l]

def KeepGreaterThan(df,col,val):
    l=(df[col]>val)
    return df[l]

def KeepGreaterEqual(df,col,val):
    l=(df[col]>=val)
    return df[l]

def KeepLowerThan(df,col,val):
    l=(df[col]<val)
    return df[l]

def KeepLowerEqual(df,col,val):
    l=(df[col]<=val)
    return df[l]

def KeepColums(df, cols):
    return df[cols]



