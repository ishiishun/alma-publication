# Import packages 
from astroquery.alma import Alma
from astroquery import nasa_ads as na
import urllib, collections, os, re, time, glob, ssl
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
ssl._create_default_https_context = ssl._create_unverified_context

def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None

def get_parse_xml(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        XmlData = response.read()
    root = ET.fromstring(XmlData)
    return root

def gettaglist(root_xml):
    taglist = []
    for items in root_xml:
        for metad in items:
            taglist.append(metad.tag)
    taglist= list(set(taglist))
    return taglist

def generateDF():
    if (os.path.exists('./{:}_df_ADS_xml.pkl'.format(na.ADS.TOKEN))):
        print('data base with {:} ADS token exists, skipping generation of the data base'.format(na.ADS.TOKEN))
        df = pd.read_pickle('./{:}_df_ADS_xml.pkl'.format(na.ADS.TOKEN))
    else:
        print('data base with {:} ADS token does not exists, generating the new data base'.format(na.ADS.TOKEN))
        url='http://telbib.eso.org/api.php?telescope[]=ALMA'
        root = get_parse_xml(url)
        numFound = int(root[0].text)
        print("Total number of entry is ", numFound)
        #taglist = gettaglist(root)
        taglist =['authors', 'year', 'programids', 'bibcode', 'journal', 'citation']
        tag_multientory = ['authors','programids']    
        def xml2df(root,df):
            for article in root[1:]:
                tmp = {}
                for metad in article:
                    for tagl in taglist:
                        if metad.tag == tagl:
                            if metad.tag in tag_multientory:
                                t = ''
                                for child in metad:
                                    # t.append(child.text.rstrip().lstrip())
                                    t += ':{:}'.format(child.text.rstrip().lstrip())
                            else:
                                t = metad.text
                            tmp.update({tagl: t})            
                df = df.append(tmp,ignore_index=True)
            return df

        df = pd.DataFrame(columns=taglist)
        df = xml2df(root,df)
        maxhit = 500
        if numFound > maxhit:
            for i in range(1,int(numFound/maxhit)+1):
                url2 = url + '&start={:}'.format(i*maxhit)
                print('Quering: {:}'.format(url2))
                root = get_parse_xml(url2)
                #print(root)
                df = xml2df(root,df)
    return df

def flatten(l):
    for el in l:
        if isinstance(el, collections.abc.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el
            


def updateprogramids(df):
    tmp = []
    for i in df.index:
        data = np.array(df['programids'].iloc[i].split(':')[1:])
        data = np.array([j.replace(' , ALMA-Partner','') for j in data])
        mask = np.array([re.match(r'20\d+',j)!=None for j in data])
        tmp.append(list(set(data[mask])))
    df['almaprojcode'] = tmp
    


def addads(df):
    if (os.path.exists('./{:}_df_ADS_xml.pkl'.format(na.ADS.TOKEN))):
        print('data base with {:} ADS token exists, skipping generation of the data base'.format(na.ADS.TOKEN))
    else:
        print('data base with {:} ADS token does not exists, generating the new data base'.format(na.ADS.TOKEN))
        doi = []
        aff = []
        cite = []
        pubdate = []
        year = []
        title = []
        bibcode = []
        authors = []
        for i in df.index:
            bib = df['bibcode'][i]
            q = na.ADS.query_simple(bib)
            q = q[q['bibcode'] == bib]
            doi_code = q['doi'][0][0] 
            if (doi_code == None):
                doi_code = 'N/A'
            doi.append(doi_code)
            bibcode.append(q['bibcode'][0])
            authors.append(q['author'][0])
            aff.append(q['aff'][0])
            if (i%500 == 0):
                print('quering....index: %d'%(i))
            cite.append(q['citation_count'][0])
            title.append(q['title'][0][0])
            (yyyy, mm, dd) = q['pubdate'][0].split('-')
            if (mm == '00'):
                mm = '01'
            if (dd == '00'):
                dd = '01'
            pubdate.append(pd.to_datetime(yyyy+'-'+mm+'-'+dd))
            year.append(yyyy)
                
        df['authors'] = authors
        df['bibcode'] = bibcode
        df['doi'] = doi
        df['affiliations'] = aff
        df['citation'] = cite
        df['pubdate'] = pubdate
        df['year'] = year
        df['title'] = title

        df.to_pickle('./{:}_df_ADS_xml.pkl'.format(na.ADS.TOKEN))
        print('./{:}_df_ADS_xml.pkl is generated'.format(na.ADS.TOKEN))

    
def ListAuthors(df):
    authlist=[]
    PIauth = [] 
    for idx in df.index:
        #authlist = df['authors'][idx].split(':')[1::]
        authlist = df['authors'][idx]
        PIauth.append(authlist[0])
    df['PI'] = PIauth
    
def index_multi(l, x):
    return [i for i, _x in enumerate(l) if _x == x]

def match_list(aff, l):
    return any((x in aff) for x in l)

def match_dict(aff, dic):
    a = [dic[x] for x in dic.keys() if (x in aff)][0]
    return a

def pick_institute(aff, keyword):
    return [x for x in aff.split(', ') if keyword in x][0]
    
    
notcountries = ['.','&amp;', '&gt;', '&lt;', 'CONICET;', 'IPAG;', 'e-mail:', 'Corresponding author', 'Deceased',
                'Private Researcher', 'These authors contributed equally to this work', 'Astrophysical Institute',
                'Department of Astronomy', 'Department of Physics']
Collaborations = ['Partnership', 'Collaboration', 'Team', 'Working Group']
UKs = ['UK', 'United Kingdom', 'DH1 3LE', 'London', 'EH9 3HJ', 'WC1E 6BT', 'CB3 0HA']
Japans = ['Japan', 'Tokyo', 'Japa', 'Nagano', 'Chiba', 'Niigata', 'Ibaraki', 'Sagamihara', 'Ehime', 'Kyoto', 'Osaka', 
          'Nagoya', 'Kanagawa', 'Hokkaido', 'Kavli Institute for The Physics of The universe', 'Subaru',
          'NAOJ', 'Chile Observatory', 'WPI', 'SOKENDAI', 'JSPS', 'Research Center for The Early Universe']
USs = ['United States', 'USA', 'CfA', 'Berkeley', 'National Radio Astronomy Observatory', 'Virginia',
       'National Optical Astronomy Observatory', 'Kavli Institute for Particle Astrophysics', 'Colorado', 'Las Vegas',
       'NASA', 'Harvard-Smithsonian', 'Villanova University', 'Maryland','University of Illinois', 'Michigan',
       'National Aeronautics and Space Administration', 'Montana', 'CIERA', 'Arizona', 'Florida', 'ASTRAVEO']
Germanys = ['Germany', 'Germnay']
Frances = ['France', 'Pessac', 'Toulouse', 'École Normale Supérieure', 'UPS-OMP', 'IRAP']
Taiwans = ['Taiwan', 'ASIAA', 'Taïwan', 'Taipei'] #'Academia Sinica'
Italys = ['Italia', 'Italy', 'Milano']
Mexicos = ['México', 'Mexico', 'MÉxico']
Austrias = ['Austria', 'Vienna']
Spains = ['Spain', 'CSIC', 'CSIC-INTA']
Australias = ['Australia', 'ASTRO 3D', 'CAASTRO']
Chiles = ['Chile', 'Santiago', 'JAO']
Brazils = ['Brasil','Brazil']
Vietnams = ['Vietnam', 'Viet Nam']
Chinas = ['China', 'Hong Kong','Chuxion']
NZs = ['New Zealand', 'NZ']
Serbias = ['Serbia', 'Beograd']
Swedens = ['Sweden', 'Onsala']
UAEs = ['UAE', 'United Arab Emirates']
SaudiArabias = ['Saudi Arabia' ,'Saudia Arabia']
Belgiums = ['Belgium', 'Belgique']
Denmarks = ['Denmark', 'DAWN', 'Lyngby']
CzechRepublics = ['Czech Republic', 'Czechia']
Netherlands = ['Netherlands', 'Nertherlands', 'Netherland']
Madagascars = ['Madagascar']
Fellows = ['Fellow', 'fellow']


def addcountries(df):
    countries = []
    for idx in df.index:
        tmp1 = []
        for auth in range(len(df['affiliations'][idx])):
            tmp2 = []
            if (df['affiliations'][idx][auth] == None):
                print(idx, df['affiliations'][idx][auth])
                countryname = 'Unknown'
                tmp2.append(countryname)
                tmp2 = [a for a in tmp2 if a != '']
            else:
                affs = df['affiliations'][idx][auth]
                affs = affs.replace('the ', 'The ')
                for w in notcountries:
                    affs = affs.replace(w,'')
                affs = affs.split(';')
                affs = [a for a in affs if a != '']
                affs = [a for a in affs if a != ' ']
                for aff in affs:
                    countryname  = aff.strip()
                    if (countryname == '-'):
                        countryname = 'Unknown'
                        for author in df['authors'][idx]:
                            if match_list(author, Collaborations):
                                countryname = 'Collaboration'
                    elif match_list(countryname, Japans):
                        countryname = 'Japan'
                    elif match_list(countryname, Taiwans):
                            countryname = 'Taiwan'
                    elif match_list(countryname, Chiles):
                        countryname = 'Chile'
                    elif match_list(countryname, Chinas):
                        countryname = 'China'
                    elif match_list(countryname, UKs):
                        countryname = 'UK'
                    elif match_list(countryname, USs):
                        countryname = 'USA'
                    elif match_list(countryname, Germanys):
                        countryname = 'Germany'
                    elif match_list(countryname, Frances):
                        countryname = 'France'
                    elif match_list(countryname, Netherlands):
                        countryname = 'Netherlands'
                    elif match_list(countryname, Mexicos):
                        countryname = 'Mexico'
                    elif match_list(countryname, Italys):
                        countryname = 'Italy'
                    elif match_list(countryname, Austrias):
                        countryname = 'Austria'
                    elif match_list(countryname, Spains):
                        countryname = 'Spain'
                    elif match_list(countryname, Australias):
                        countryname = 'Australia'
                    elif match_list(countryname, Vietnams):
                        countryname = 'Vietnam'
                    elif match_list(countryname, Brazils):
                        countryname = 'Brazil'
                    elif match_list(countryname, CzechRepublics):
                        countryname = 'Czech Republic'
                    elif match_list(countryname, Madagascars):
                        countryname = 'Madagascar'
                    elif 'Canada' in countryname:
                        countryname = 'Canada'
                    elif 'Korea' in countryname:
                        countryname = 'Korea'
                    elif match_list(countryname, Denmarks):
                        countryname = 'Denmark'
                    elif ('Indonesia' in countryname):
                        countryname = 'Indonesia'
                    elif 'Malaysia' in countryname:
                        countryname = 'Malaysia'
                    elif match_list(countryname, Serbias):
                        countryname = 'Serbia'
                    elif match_list(countryname, NZs):
                        countryname = 'New Zealand'                
                    elif ('Poland' in countryname):
                        countryname = 'Poland'
                    elif ('Thailand' in countryname):
                        countryname = 'Thailand'
                    elif ('South Africa' in countryname):
                        countryname = 'South Africa'                
                    elif ('Nigeria' in countryname):
                        countryname = 'Nigeria'
                    elif 'Russia' in countryname:
                        countryname = 'Russia'
                    elif 'India' in countryname:
                        countryname = 'India'
                    elif match_list(countryname, Swedens):
                        countryname = 'Sweden'
                    elif 'Norway' in countryname:
                        countryname = 'Norway'
                    elif 'Switzerland' in countryname:
                        countryname = 'Switzerland'
                    elif 'Argentina' in countryname:
                        countryname = 'Argentina'
                    elif match_list(countryname, Belgiums):
                        countryname = 'Belgium'
                    elif 'Finland' in countryname:
                        countryname = 'Finland'
                    elif 'Hungary' in countryname:
                        countryname = 'Hungary'
                    elif match_list(countryname, SaudiArabias):
                        countryname = 'Saudi Arabia'
                    elif 'Colombia' in countryname:
                        countryname = 'Colombia'
                    elif 'Greece' in countryname:
                        countryname = 'Greece'
                    elif 'Croatia' in countryname:
                        countryname = 'Croatia'
                    elif 'Portugal' in countryname:
                        countryname = 'Portugal'
                    elif 'Israel' in countryname:
                        countryname = 'Israel'
                    elif 'Ireland' in countryname:
                        countryname = 'Ireland'
                    elif 'Iraq' in countryname:
                        countryname = 'Iraq'
                    elif ('Swarnajayanti' in countryname): 
                        countryname = 'India'
                    elif ('Ukraine' in countryname): 
                        countryname = 'Ukraine'
                    elif ('Bulgaria' in countryname): 
                        countryname = 'Bulgaria'
                    elif ('Uzbekistan' in countryname): 
                        countryname = 'Uzbekistan'
                    elif ('Georgia' in countryname): 
                        countryname = 'Georgia'
                    elif ('Kazakhstan' in countryname): 
                        countryname = 'Kazakhstan'
                    elif ('Iceland' in countryname): 
                        countryname = 'Iceland'
                    elif ('Latvia' in countryname): 
                        countryname = 'Latvia'
                    elif match_list(countryname, UAEs):
                        countryname = 'UAE'
                    elif ('Turkey' in countryname): 
                        countryname = 'Turkey'
                    elif ('Ethiopia' in countryname): 
                        countryname = 'Ethiopia'
                    elif ('Vatican State' in countryname): 
                        countryname = 'Vatican State'
                    elif ('Tunisia' in countryname): 
                        countryname = 'Tunisia'
                    elif ('Cyprus' in countryname): 
                        countryname = 'Cyprus'
                    elif ('Slovenia' in countryname): 
                        countryname = 'Slovenia'
                    elif ('Armenia' in countryname): 
                        countryname = 'Armenia'
                    elif ('Crimea' in countryname): 
                        countryname = 'Crimea'
                    elif ('Estonia' in countryname): 
                        countryname = 'Estonia'
                    elif ('Namibia' in countryname): 
                        countryname = 'Namibia'
                    elif ('Burkina Faso' in countryname): 
                        countryname = 'Burkina Faso'
                    elif ('Kenya' in countryname): 
                        countryname = 'Kenya'
                    elif ('Tanzania' in countryname): 
                        countryname = 'Tanzania'
                    elif match_list(countryname, Fellows):
                        countryname = ''
                    elif ('@' in countryname): 
                        countryname = ''
                    elif re.match(r'\d+', countryname): #or ('' in countryname):
                        try:
                            countryname2 = aff[-2]
                            print(idx,auth,countryname, end='->')
                            countryname = countryname2.strip()
                            print(countryname)
                        except IndexError:
                            print(idx,auth,'IndexError',aff)
                    else:
                        countryname = 'Unknown'
                        print(idx, countryname, df['affiliations'][idx][auth])
                    tmp2.append(countryname)
                    tmp2 = [a for a in tmp2 if a != '']
            tmp1.append(tmp2)
        countries.append(tmp1)
    df['countries'] = countries
    
    
def addcountries_ESO(df):
    countries = []
    for idx in df.index:
        tmp1 = []
        for auth in range(len(df['affiliations'][idx])):
            tmp2 = []
            if (df['affiliations'][idx][auth] == None):
                countryname = 'Unknown'
                tmp2.append(countryname)
                tmp2 = [a for a in tmp2 if a != '']
            else:
                affs = df['affiliations'][idx][auth]
                affs = affs.replace('the ', 'The ')
                for w in notcountries:
                    affs = affs.replace(w,'')
                affs = affs.split(';')
                affs = [a for a in affs if a != '']
                affs = [a for a in affs if a != ' ']
                for aff in affs:
                    countryname  = aff.strip()
                    if (countryname == '-'):
                        if any(['ALMA' in  author for author in df['authors'][idx]]):
                            countryname = 'ALMA'
                        elif any(['Event Horizon Telescope' in  author for author in df['authors'][idx]]):
                            countryname = 'EHT'
                        elif any(['SKA' in  author for author in df['authors'][idx]]):
                            countryname = 'SKA'
                        elif any(['WEBT' in  author for author in df['authors'][idx]]):
                            countryname = 'WEBT'
                        else:
                            for author in df['authors'][idx]:
                                if match_list(author, Collaborations):
                                    countryname = 'Collaboration'
                    elif match_list(countryname, Japans):
                        countryname = 'Japan'
                    elif match_list(countryname, Taiwans):
                            countryname = 'Taiwan'
                    elif match_list(countryname, Chiles):
                        countryname = 'Chile'
                    elif match_list(countryname, Chinas):
                            countryname = 'China'
                    elif match_list(countryname, UKs):
                        countryname = 'UK'
                    elif match_list(countryname, USs):
                        countryname = 'USA'
                    elif match_list(countryname, Germanys):
                        countryname = 'Germany'
                    elif match_list(countryname, Mexicos):
                        countryname = 'Mexico'
                    elif match_list(countryname, Frances):
                        countryname = 'France'
                    elif match_list(countryname, Netherlands):
                        countryname = 'Netherlands'
                    elif match_list(countryname, Italys):
                        countryname = 'Italy'
                    elif match_list(countryname, Austrias):
                        countryname = 'Austria'
                    elif match_list(countryname, Spains):
                        countryname = 'Spain'
                    elif match_list(countryname, Australias):
                        countryname = 'Australia'
                    elif match_list(countryname, Vietnams):
                        countryname = 'Vietnam'
                    elif match_list(countryname, Brazils):
                        countryname = 'Brazil'
                    elif match_list(countryname, CzechRepublics):
                        countryname = 'Czech Republic'
                    elif match_list(countryname, Madagascars):
                        countryname = 'Madagascar'
                    elif 'Canada' in countryname:
                        countryname = 'Canada'
                    elif 'Korea' in countryname:
                        countryname = 'Korea'
                    elif match_list(countryname, Denmarks):
                        countryname = 'Denmark'
                    elif ('Indonesia' in countryname):
                        countryname = 'Indonesia'
                    elif 'Malaysia' in countryname:
                        countryname = 'Malaysia'
                    elif match_list(countryname, Serbias):
                        countryname = 'Serbia'
                    elif match_list(countryname, NZs):
                        countryname = 'New Zealand'                
                    elif ('Poland' in countryname):
                        countryname = 'Poland'
                    elif ('Thailand' in countryname):
                        countryname = 'Thailand'
                    elif ('South Africa' in countryname):
                        countryname = 'South Africa'                
                    elif ('Nigeria' in countryname):
                        countryname = 'Nigeria'
                    elif 'Russia' in countryname:
                        countryname = 'Russia'
                    elif 'India' in countryname:
                        countryname = 'India'
                    elif match_list(countryname, Swedens):
                        countryname = 'Sweden'
                    elif 'Norway' in countryname:
                        countryname = 'Norway'
                    elif 'Denmark' in countryname:
                        countryname = 'Denmark'
                    elif 'Switzerland' in countryname:
                        countryname = 'Switzerland'
                    elif 'Argentina' in countryname:
                        countryname = 'Argentina'
                    elif ('Bulgaria' in countryname): 
                        countryname = 'Bulgaria'
                    elif 'Finland' in countryname:
                        countryname = 'Finland'
                    elif 'Hungary' in countryname:
                        countryname = 'Hungary'
                    elif match_list(countryname, SaudiArabias):
                        countryname = 'Saudi Arabia'
                    elif 'Colombia' in countryname:
                        countryname = 'Colombia'
                    elif 'Greece' in countryname:
                        countryname = 'Greece'
                    elif 'Croatia' in countryname:
                        countryname = 'Croatia'
                    elif 'Portugal' in countryname:
                        countryname = 'Portugal'
                    elif 'Israel' in countryname:
                        countryname = 'Israel'
                    elif 'Ireland' in countryname:
                        countryname = 'Ireland'
                    elif 'Czech Republic' in countryname:
                        countryname = 'Czech Republic'
                    elif 'Iraq' in countryname:
                        countryname = 'Iraq'
                    elif ('Swarnajayanti' in countryname): 
                        countryname = 'India'
                    elif ('Ukraine' in countryname): 
                        countryname = 'Ukraine'
                    elif match_list(countryname, Belgiums):
                        countryname = 'Belgium'
                    elif ('Uzbekistan' in countryname): 
                        countryname = 'Uzbekistan'
                    elif ('Georgia' in countryname): 
                        countryname = 'Georgia'
                    elif ('Kazakhstan' in countryname): 
                        countryname = 'Kazakhstan'
                    elif ('Iceland' in countryname): 
                        countryname = 'Iceland'
                    elif ('Latvia' in countryname): 
                        countryname = 'Latvia'
                    elif match_list(countryname, UAEs):
                        countryname = 'UAE'
                    elif ('Turkey' in countryname): 
                        countryname = 'Turkey'
                    elif ('Ethiopia' in countryname): 
                        countryname = 'Ethiopia'
                    elif ('Vatican State' in countryname): 
                        countryname = 'Vatican State'
                    elif ('Tunisia' in countryname): 
                        countryname = 'Tunisia'
                    elif ('Cyprus' in countryname): 
                        countryname = 'Cyprus'
                    elif ('Slovenia' in countryname): 
                        countryname = 'Slovenia'
                    elif ('Armenia' in countryname): 
                        countryname = 'Armenia'
                    elif ('Crimea' in countryname): 
                        countryname = 'Crimea'
                    elif ('Estonia' in countryname): 
                        countryname = 'Estonia'
                    elif ('Namibia' in countryname): 
                        countryname = 'Namibia'
                    elif ('Burkina Faso' in countryname): 
                        countryname = 'Burkina Faso'
                    elif ('Kenya' in countryname): 
                        countryname = 'Kenya'
                    elif ('Tanzania' in countryname): 
                        countryname = 'Tanzania'
                    elif match_list(countryname, Fellows):
                        countryname = ''
                    elif ('@' in countryname): 
                        countryname = ''
                    elif re.match(r'\d+', countryname): #or ('' in countryname):
                        try:
                            countryname2 = aff[-2]
                            print(idx,auth,countryname, end='->')
                            countryname = countryname2.strip()
                            print(countryname)
                        except IndexError:
                            print(idx,auth,'IndexError',aff)
                    else:
                        countryname = 'Unknown'
                        print(idx, countryname, df['affiliations'][idx][auth])
                    tmp2.append(countryname)
                    tmp2 = [a for a in tmp2 if a != '']
            tmp1.append(tmp2)
        countries.append(tmp1)
    df['countries_ESO'] = countries



# Manually insert
def updatedf_countries(bibcode, countrylist, df):
    rowidx = df.index[df['bibcode']==bibcode][0]
    df.countries[int(rowidx)] = countrylist
    df.countries_ESO[int(rowidx)] = countrylist
    
    
def addFirstcountry(df):
    vallist = []
    for idx in df.index:
        PIcountries = df['countries'][idx][0]
        if ('Japan' in PIcountries):
            vallist.append('Japan')
        else:
            vallist.append(PIcountries[0])
    df['Firstcountry'] = vallist
    
def addFirstcountry_ESO(df):
    vallist = []
    for idx in df.index:
        PIcountries = df['countries_ESO'][idx][0]
        if (PIcountries[0] == 'ESO'):
            if (len(PIcountries) == 1):
                vallist.append('ESO')
            else:
                vallist.append(PIcountries[1])
        else:
            vallist.append(PIcountries[0])
    df['Firstcountry_ESO'] = vallist
    
    
def addJapaneseAff2(df):
        vallist = []
        for idx in df.index:
            PIcountries = df['countries'][idx][0]
            IsPIJapanese = 'Japan' in PIcountries
            if (IsPIJapanese == True) & (df['countries'][idx][0][0] != 'Japan'):
                print(idx, df['countries'][idx][0])
                vallist.append(df['countries'][idx][0][0])
        return pd.DataFrame({'country':vallist})
        
        
ISASs = ['Institute of Space and Astronautical Science', 'ISAS', 'Japan Aerospace Exploration Agency', 'JAXA']
NINSs = ['National PIJapaneseAffiliations of Natural Sciences', 'National PIJapaneseAffiliations of Natural Science',
        'NINS']
NAOJs = ['National Astronomical Observatory of Japan', 
         'National Astronomical Observatory', 'Chile Observatory', 
         'Nobeyama', 'Mizusawa', 'Subaru Telescope','Joint ALMA Observatory', 'NAOJ Fellow'] 
UTokyos = ['the University of Tokyo', 'University of Tokyo']

def addJapanese_aff(df):
        vallist_PIJapaneseAffiliation = []
        vallist_PIaffiliation = []
        for idx in df.index:
            PIJapaneseAffiliation = None
            PIaffiliation = None
            PIcountries = df['countries'][idx][0]
            PIaffs = df['affiliations'][idx][0].replace('&amp;','').replace('&gt;','').replace('CONICET;','').replace('IPAG;','').split(';')
            PIaffs = [a for a in PIaffs if a != '']
            PIaffs = [a for a in PIaffs if a != ' ']
        
            IsPIJapanese = 'Japan' in PIcountries
            if (IsPIJapanese == True):
                indexJapaneseAffiliation = index_multi(PIcountries, 'Japan')[0]
                JapaneseAffiliation = PIaffs[indexJapaneseAffiliation].strip()
                if match_list(JapaneseAffiliation, Fellows):
                    indexJapaneseAffiliation = index_multi(PIcountries, 'Japan')[1]
                    JapaneseAffiliation = PIaffs[indexJapaneseAffiliation].strip()
                else:
                    pass
                if ('SOKENDAI' in JapaneseAffiliation):
                    PIJapaneseAffiliation = 'SOKENDAI'
                elif ('RIKEN' in JapaneseAffiliation):
                    PIJapaneseAffiliation = 'RIKEN'
                elif ('NEC Corporation Fuchu' in JapaneseAffiliation):
                    PIJapaneseAffiliation = 'NEC Corporation Fuchu'
                elif  match_list(JapaneseAffiliation, ISASs):
                    PIJapaneseAffiliation = 'ISAS/JAXA'
                elif match_list(JapaneseAffiliation, NAOJs):
                    PIJapaneseAffiliation = 'NAOJ'
                elif ('Astrobiology Center' in JapaneseAffiliation):
                    PIJapaneseAffiliation = 'Astrobiology Center'
                elif all((s in JapaneseAffiliation) for s in NINSs):
                    PIJapaneseAffiliation = 'NINS'
                elif('Kavli' in JapaneseAffiliation) or ('IPMU' in JapaneseAffiliation): 
                    PIJapaneseAffiliation = 'IPMU'
                    PIaffiliation = 'The University of Tokyo (incl. IPMU and ICRR)'
                elif('Institute for Cosmic Ray Research' in JapaneseAffiliation) or ('ICRR' in JapaneseAffiliation): 
                    PIJapaneseAffiliation = 'ICRR'
                    PIaffiliation = 'The University of Tokyo (incl. IPMU and ICRR)'
                elif match_list(JapaneseAffiliation, UTokyos):
                    PIJapaneseAffiliation = 'The University of Tokyo'
                    PIaffiliation = 'The University of Tokyo (incl. IPMU and ICRR)'
                elif ('University' in JapaneseAffiliation):
                    PIJapaneseAffiliation = pick_institute(JapaneseAffiliation, 'University')
                elif ('Institute' in JapaneseAffiliation):
                    PIJapaneseAffiliation = pick_institute(JapaneseAffiliation, 'Institute')
                elif ('High School' in JapaneseAffiliation):
                    PIJapaneseAffiliation = pick_institute(JapaneseAffiliation, 'High School')
                elif ('-' in JapaneseAffiliation):
                    print(idx, df['bibcode'][idx], JapaneseAffiliation)
                else:
                    print(idx, df['bibcode'][idx], JapaneseAffiliation, indexJapaneseAffiliation, PIaffs)
                    #pass
            vallist_PIJapaneseAffiliation.append(PIJapaneseAffiliation)
            if (PIaffiliation == None):
                vallist_PIaffiliation.append(PIJapaneseAffiliation)
            else:
                vallist_PIaffiliation.append(PIaffiliation)
        
        df['PIJapaneseAffiliation'] = vallist_PIJapaneseAffiliation
        df['PIAffiliation'] = vallist_PIaffiliation
        
def updatedf_PIJapaneseAffiliation(bibcode, PIJapaneseAffiliation, PIAffiliation, df):
    rowidx = df.index[df['bibcode']==bibcode][0]
    df.loc[int(rowidx), 'PIJapaneseAffiliation'] = PIJapaneseAffiliation
    df.loc[int(rowidx), 'PIAffiliation'] = PIAffiliation
    

ESOs = ['European Southern Observatory', 'ESO']
NRAOs = ['National Radio Astronomy Observatory', 'NRAO', 'National Radio Astronomy Observtory']
ASIAAs = ['ASIAA', 'Academia Sinica']
NRCs = ['Herzberg Institute of Astrophysics','National Research Council of Canada', 'NRC', 'National Research Council Canada']
JAOs = ['Atacama Large Millimeter/submillimeter Array', 'Joint ALMA', 'JAO']
KASIs = ['KASI', 'Korea Astronomy']
MPls = ['Max-Planck', 'Max Planck', 'MPI']
CfAs = ['Harvard-Smithsonian', 'CfA', 'Center for Astrophysics ∣ Harvard  Smithsonian', 'Harvard']
UCs = ['University of California', 'UCLA']
UCLs = ['UCL', 'University College London']
Caltechs = ['Caltech', 'California Institute of Technology']
MITs = ['Kavli Institute for Astrophysics and Space Research', 'Massachusetts Institute of Technology', 'MIT']
UBCs = ['University of British Columbia', 'Department of Physics and Astronomy, 6224 Agricultural Road, Vancouver']
IRAMs = ['Institut de Radioastronomie', 'Institut de RadioAstronomie', 'IRAM', 'Institut de Radio Astronomie Millimétrique']
LESIAs = ['LESIA', 'Observatoire de Paris']
CEAs = ['Laboratoire de Physique des Lasers', 'CEA']
IACs = ['IAC', 'Instituto de Astrofísica de Canarias']
UPMCs = ['UPMC', 'Sorbonne University']
ASTRONs = ['ASTRON', 'Netherlands Institute for Radio Astronomy']
CSIROs = ['CSIRO']
Lilles = ['Lille']
UPSs= ['Paris-Sud']
Oxfords = ['Oxford, OX1']
Yales = ['Yale Center for Astronomy and Astrophysics', 'Yale University']
Grenobles = ['University Grenoble Alpes', 'Université Grenoble Alpes']
Toulouses = ['Université de Toulouse', 'University Toulouse']
AMUs = ['Aix Marseille Université', 'Aix-Marseille University', 'Aix Marseille Univ']
STScIs = ['STScI', 'Space Telescope Science Institute']
Leidens = ['Leiden Observatory', 'Leiden University']
NBIs = ['Niels Bohr', 'NBI', 'University of Copenhagen', 'Cosmic Dawn Center']
IPAs = ['Institute of Applied Physics of the Russian Academy of Sciences', 'Russian Academy of Sciences']
TheUniversities = {'Bordeaux': 'Université de Bordeaux', 
                   'University of Arizona': 'The University of Arizona', 
                   'University of New Mexico':'The University of New Mexico',
                  'Ohio State University': 'The Ohio State University',
                  'University of Manchester': 'The University of Manchester',
                  'University of Sheffield' : 'The University of Sheffield',
                  'University of Western Australia': 'The University of Western Australia',
                  'State University of New Jersey': 'The State University of New Jersey',
                  'University of Leeds': 'University of Leeds',
                  'University of Hong Kong': 'The University of Hong Kong',
                  'University of Texas':'The University of Texas'}
Bordeauxs = ['Bordeaux']
Arizonas = ['University of Arizona']
NewMexicos = ['University of New Mexico']
SISSAs = ['SISSA', 'SISSA – ISAS']
Ohios = ['Ohio State University']
Cambridges = ['Institute of Astronomy, Madingley Road', 'Cavendish Laboratory']
Manchesters = ['University of Manchester']
NAOCs = ['NAOC', 'National Astronomical Observatories', 'Yunnan Observatories']
NASAs = ['NASA', 'IPAC']
KULeuvens = ['KU Leuven', 'Katholieke Universiteit Leuven']
Universities = ['University', 'Univ.', 'Universidad', 'Universität', 'Università', 'Université', 'Universitá', 'Universiteit', 'Universitat', 'Universit']
Institutes = ['Institute', 'Instituto', 'Instituut', 'Institut']
Observatories = ['Observatory', 'Osservatorio', 'Observatorio', 'Observatoire', 'Sterrenwacht']
Colleges = ['College']
Schools = ['Scuola', 'School']
Fellows = ['Fellow', 'fellow']


def addAff(df):
        vallist = []
        for idx in df.index:
            PIcountries = df['countries'][idx][0]
            PIaffs = df['affiliations'][idx][0].replace('&amp;','').replace('&gt;','').replace('CONICET;','').replace('IPAG;','').split(';')
            PIaffs = [a for a in PIaffs if a != '']
            PIaffs = [a for a in PIaffs if a != ' ']
            IsPIJapanese = 'Japan' in PIcountries
            PIaffiliation = df['PIAffiliation'][idx]
            if (df['PIAffiliation'][idx] == None):
                affiliation = PIaffs[0].strip()
                if (('Postdoctoral Fellow' in affiliation) and (len(PIaffs)>1)):
                    affiliation = PIaffs[1].strip()
                elif (match_list(affiliation, Fellows) and (len(PIaffs)>1)):
                    affiliation = PIaffs[1].strip()
                elif (('author' in affiliation) and (len(PIaffs)>1)):
                    affiliation = PIaffs[1].strip()
                elif (('@' in affiliation) and (len(PIaffs)>1)):
                    affiliation = PIaffs[1].strip()
                else:
                    pass
                affiliation = affiliation.replace('Univ.', 'University')
                affiliation = affiliation.replace('Univ ', 'University ')
                if (affiliation == '-'):
                    PI = df['authors'][idx][0]
                    if ('Partnership' in PI) or ('Collaboration' in PI) or ('Working Group' in PI):
                        PIaffiliation = PI
                    else:
                        PIaffiliation = 'Unknown'
                elif match_list(affiliation, ESOs):
                    PIaffiliation = 'ESO'
                elif match_list(affiliation, NRAOs):
                    PIaffiliation = 'NRAO'
                elif match_list(affiliation, ASIAAs):
                    PIaffiliation = 'ASIAA'
                elif match_list(affiliation, NRCs): 
                    PIaffiliation = 'NRC-HIA'
                elif match_list(affiliation, MPls):
                    PIaffiliation = 'Max Planck Institute'
                elif match_list(affiliation, JAOs): 
                    PIaffiliation = 'Joint ALMA Observatory'
                elif match_list(affiliation, CfAs):
                    PIaffiliation = 'Harvard-Smithsonian CfA'
                elif match_list(affiliation, Leidens):
                    PIaffiliation = 'Leiden Observatory/Leiden University'
                elif match_list(affiliation, UCs):
                    PIaffiliation = 'University of California'
                elif match_list(affiliation, UCLs):
                    PIaffiliation = 'University College London'
                elif match_list(affiliation, Caltechs):
                    PIaffiliation = 'California Institute of Technology'
                elif match_list(affiliation, Yales):
                    PIaffiliation = 'Yale University'
                elif match_list(affiliation, LESIAs):
                    PIaffiliation = 'Observatoire de Paris'
                elif match_list(affiliation, AMUs):
                    PIaffiliation = 'Aix Marseille Université'
                elif match_list(affiliation, CEAs):
                    PIaffiliation = 'CEA'
                elif match_list(affiliation, IRAMs):
                    PIaffiliation = 'IRAM'
                elif match_list(affiliation, UPSs):
                    PIaffiliation = 'Université Paris-Sud'
                elif match_list(affiliation, KASIs):
                    PIaffiliation = 'KASI'
                elif match_list(affiliation, UPMCs):
                    PIaffiliation = 'Sorbonne University'
                elif match_list(affiliation, UBCs):
                    PIaffiliation = 'The University of British Columbia'
                elif match_list(affiliation, Bordeauxs):
                    PIaffiliation = 'Université de Bordeaux'
                elif match_list(affiliation, Lilles): 
                    PIaffiliation = 'Université de Lille'
                elif match_list(affiliation, Grenobles): 
                    PIaffiliation = 'Université Grenoble Alpes'
                elif match_list(affiliation, Toulouses): 
                    PIaffiliation = 'Université de Toulouse'
                elif match_list(affiliation, IACs): 
                    PIaffiliation = 'Instituto de Astrofísica de Canarias'
                elif match_list(affiliation, MITs): 
                    PIaffiliation = 'Massachusetts Institute of Technology'
                elif match_list(affiliation, Oxfords): 
                    PIaffiliation = 'Universityersity of Oxford'
                elif match_list(affiliation, STScIs): 
                    PIaffiliation = 'STScl'
                elif match_list(affiliation, ASTRONs): 
                    PIaffiliation = 'ASTRON'
                elif match_list(affiliation, CSIROs): 
                    PIaffiliation = 'CSIRO'
                elif match_list(affiliation, IPAs): 
                    PIaffiliation = 'Institute of Applied Physics of the Russian Academy of Sciences'
                elif match_list(affiliation, NBIs): 
                    PIaffiliation = 'Niels Bohr Institute/University of Copenhagen'
                elif match_list(affiliation, SISSAs): 
                    PIaffiliation = 'SISSA, Italy'
                elif match_list(affiliation, NAOCs): 
                    PIaffiliation = 'National Astronomical Observatories, Chinese Academy of Science'
                elif match_list(affiliation, TheUniversities.keys()):
                    PIaffiliation = match_dict(affiliation, TheUniversities)
                elif match_list(affiliation, NASAs): 
                    PIaffiliation = 'NASA'
                elif ('ESA' in affiliation):
                    PIaffiliation = 'ESA'
                elif ('Jodrell Bank' in affiliation):
                    PIaffiliation = 'Jodrell Bank Centre for Astrophysics'
                elif ('Square Kilometre Array' in affiliation):
                    PIaffiliation =  'Square Kilometre Array Organisation'
                elif('UNAM' in affiliation): 
                    PIaffiliation = 'Universidad Nacional Autónoma de México'
                elif('CSIC' in affiliation): 
                    PIaffiliation = 'CSIC (Spain)'
                elif('Carnegie Institution for Science' in affiliation): 
                    PIaffiliation = 'Carnegie Institution for Science'
                elif('INAF'  in affiliation): 
                    PIaffiliation = 'INAF (Italy)'
                elif('Chalmers University of Technology' in affiliation): 
                    PIaffiliation = 'Chalmers University of Technology'
                elif match_list(affiliation, Cambridges): 
                    PIaffiliation = 'University of Cambridge'
                elif('University of Michigan' in affiliation): 
                    PIaffiliation = 'University of Michigan'
                elif('Vietnam National' in affiliation):
                    PIaffiliation = 'Vietnam National Satellite Center'
                elif('ETH' in affiliation): 
                    PIaffiliation = 'ETH Zürich'
                elif('Lawrence Berkeley National' in affiliation): 
                    PIaffiliation = 'Lawrence Berkeley National Laboratory'
                elif('Zagreb' in affiliation): 
                    PIaffiliation = 'University of Zagreb'
                elif match_list(affiliation, KULeuvens): 
                    PIaffiliation = 'KU Leuven'
                elif('Chip Computers Consulting' in affiliation): 
                    PIaffiliation = 'Chip Computers Consulting'
                elif('IBM Research Division' in affiliation): 
                    PIaffiliation = 'IBM Research Division'
                elif('Physical Research laboratory' in affiliation): 
                    PIaffiliation = 'Physical Research laboratory, India'
                elif('Center for Interdisciplinary Exploration and Research in Astronomy' in affiliation): 
                    PIaffiliation = 'Northwestern University'
                elif('Indian Centre for Space Physics' in affiliation): 
                    PIaffiliation = 'Indian Centre for Space Physics'
                elif('Lockheed Martin Solar' in affiliation): 
                    PIaffiliation = 'Lockheed Martin Solar and Astrophysics Laboratory'        
                elif('UK Astronomy Technology Centre' in affiliation): 
                    PIaffiliation = 'UK Astronomy Technology Centre'
                elif('Nicolaus Copernicus Astronomical Center' in affiliation): 
                    PIaffiliation = 'Nicolaus Copernicus Astronomical Center'
                elif('National Centre for Nuclear Research' in affiliation): 
                    PIaffiliation = 'National Centre for Nuclear Research'
                elif('Thüringer Landessternwarte Tautenburg' in affiliation): 
                    PIaffiliation = 'Thüringer Landessternwarte Tautenburg'
                elif('ARIES' in affiliation): 
                    PIaffiliation = 'Aryabhatta Research Institute of Observational Sciences'
                elif('Physical Research Laboratory' in affiliation): 
                    PIaffiliation = 'Physical Research Laboratory'
                elif('New Mexico Tech' in affiliation): 
                    PIaffiliation = 'New Mexico Tech'
                elif match_list(affiliation, Universities):
                    affiliation_list = affiliation.split(', ')
                    for l in affiliation_list:
                        if match_list(l, Universities):
                            PIaffiliation = l
                elif match_list(affiliation, Institutes):
                    affiliation_list = affiliation.split(', ')
                    for l in affiliation_list:
                        if match_list(l, Institutes):
                            PIaffiliation = l
                elif match_list(affiliation, Observatories):
                    affiliation_list = affiliation.split(', ')
                    for l in affiliation_list:
                        if match_list(l, Observatories):
                            PIaffiliation = l
                elif match_list(affiliation, Colleges):
                    affiliation_list = affiliation.split(', ')
                    for l in affiliation_list:
                        if match_list(l, Colleges):
                            PIaffiliation = l
                elif match_list(affiliation, Schools):
                    affiliation_list = affiliation.split(', ')
                    for l in affiliation_list:
                        if match_list(l, Schools):
                            PIaffiliation = l
                else:
                    PIaffiliation = 'Unknown'
            vallist.append(PIaffiliation)
        
        df['PIAffiliation'] = vallist
        
        
def updatedf_PIAffiliation(bibcode, PIAffiliation, df):
    rowidx = df.index[df['bibcode']==bibcode][0]
    df.loc[int(rowidx), 'PIAffiliation'] = PIAffiliation
    
    
def addCoauthJapanese(df):
    vallist = []
    for idx in df.index:
        if(len(df['countries'][idx]) < 2):
            vallist.append(False)
        else:
            Coauthcountries = list(flatten(df['countries'][idx][1::]))
            CoauthJapanese = 'Japan' in Coauthcountries
            vallist.append(CoauthJapanese)
    df['CoauthJapanese'] = vallist
    
    
    
def addJapaneseCoauthOnly(df):
    df['JapaneseCoauthOnly'] = list(df['Firstcountry'] != 'Japan') &(df['CoauthJapanese']==True)
    
    
def addFaCoJapanse(df):
    vallist = []
    for idx in df.index:
            if (df['Firstcountry'][idx] == 'Japan'):
                vallist.append('first-authored')
            elif (df['JapaneseCoauthOnly'][idx] == True):
                vallist.append('co-authored')
            else:
                vallist.append('non-Japanese')
    df['FaCoJapanese'] = vallist