import requests,json,os,re,argparse
import pandas as pd
from time import sleep

parser=argparse.ArgumentParser()
parser.add_argument('-i','--input_file', required=True, help='Input csv file with user name and orcid id')
parser.add_argument('-o','--output_xml', required=True, help='Output xml file')
args=parser.parse_args()

input_file=args.input_file
output_xml=args.output_xml


def get_pmc_data(orcid_id,cursor=''):
    '''
    A method for fetching pmc data
    
    :param orcid_id: An orcid id
    :param cursor: A cursor string, default empty string
    '''
    try:
        data=list()
        url_str='https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=AUTHORID:{0}&format=json&sort_date:y%20BDESC&cursorMark={1}'.format(orcid_id,cursor)
        response=requests.get(url_str)
        if response.ok:
            json_data=json.loads(response.content.decode('utf-8'))
            data=json_data['resultList']['result']
            #print(json_data)
            if 'nextCursorMark' in json_data:
                if cursor !=json_data['nextCursorMark']:
                    cursor=json_data['nextCursorMark']
                else:
                    cursor=''


        return data,cursor
    except:
        raise

def add_pmc_link(series):
    '''
    A method for adding pubmed link to the data table
    
    :param series: A data series with 'pmid' (pubmed id)
    '''
    try:
        pmid=series['pmid']
        series['link']='https://www.ncbi.nlm.nih.gov/pubmed/{0}'.format(pmid)
        return series
    except:
        raise


def get_pmc_data_for_user(user,orcid_id):
    '''
    A method for fetching all publication info for a user
    
    :param user: A user name
    :param orcid_id: An orcid id for PMC lookup
    :returns: A dataframe containing list of publications
    '''
    try:
        all_data=list()
        cursor=''
        while True:
            data,cursor=get_pmc_data(orcid_id=orcid_id,cursor=cursor)
            if len(data)>0 or cursor !='':
                all_data.extend(data)
                sleep(10)
            else:
                break
            
        all_data=pd.DataFrame(all_data)
        all_data['user']=user
        all_data=all_data.apply(lambda x: add_pmc_link(series=x),
                            axis=1)
        return all_data
    except:
        raise
        

def get_publication_list(input_file):
    '''
    A method for fetching publication list and writing it to an output csv file
    
    :param input_file: An input csv file containing 'name' and 'orcid' column
    returns: A pandas dataframe containing publication info of all the users
    '''
    try:
        final_data=pd.DataFrame()
        input_data=pd.read_csv(input_file).to_dict(orient='records')
        for line in input_data:
            user=line['name']
            orcid_id=line['orcid']
            user_data=get_pmc_data_for_user(user=user,
                                            orcid_id=orcid_id)
            final_data=pd.concat([final_data,
                                  user_data])
        
        keyword_list=['user','authorString','title','pubYear','firstPublicationDate','pmid','citedByCount','doi','journalTitle','journalIssn','journalVolume','pageInfo','link']
        final_data=final_data.\
                   sort_values('firstPublicationDate',
                               ascending=False)[keyword_list]

        return final_data
    except:
        raise

def make_xml(series):
    '''
    A function for formatting publication records to xml files
    '''
    try:
        output_list=["<record>"]
        title_block=["<titles>"]
        for column,value in series.items():
            if column=='authorString':
                value=re.sub(r'\.$','',value)
                author_list=value.split(',')
                if len(author_list)>0:
                    output_list.append("<contributors>")
                    output_list.append("<authors>")
                    for name in author_list:
                        name_cmp=name.split()
                        name=', '.join(name_cmp)
                        output_list.append('<author><style face="normal" font="default" size="100%">{0}.</style></author>'.format(name.strip()))
                    output_list.append("</authors>")
                    output_list.append("</contributors>")
            elif column=='title':
                title_block.append('<title><style face="normal" font="default" size="100%">{0}</style></title>'.format(value))
            elif column=='journalTitle':
                output_list.append('<periodical><full-title><style face="normal" font="default" size="100%">{0}</style></full-title></periodical>'.format(value))
                title_block.append('<secondary-title><style face="normal" font="default" size="100%">{0}</style></secondary-title>'.format(value))
            elif column=='pageInfo':
                output_list.append('<pages><style face="normal" font="default" size="100%">{0}</style></pages>'.format(value))
            elif column=='journalVolume':
                output_list.append('<volume><style face="normal" font="default" size="100%">{0}</style></volume>'.format(value))
            elif column=='journalIssn':
                output_list.append('<isbn><style face="normal" font="default" size="100%">{0}</style></isbn>'.format(value))
            elif column=='doi':
                output_list.append('<electronic-resource-num><style face="normal" font="default" size="100%">{0}</style></electronic-resource-num>'.format(value))
            elif column=='link':
                output_list.append('<urls><related-urls><url><style face="normal" font="default" size="100%">{0}</style></url></related-urls></urls>'.format(value))
            elif column=='pubYear':
                output_list.append('<dates><year><style face="normal" font="default" size="100%">{0}</style></year></dates>'.format(value))
    
        title_block.append("</titles>")
        output_list.extend(title_block)
        output_list.append("</record>")
    
        return output_list
    except:
        raise

try:
        data=get_publication_list(input_file=input_file)            # get publication list
        data=data[data['pubYear'].astype(int)>=2015]                #  filter list
        xml_list=data.apply(lambda x:make_xml(series=x),
                            axis=1)                                 # get xml blocks
        xml_data=["<xml><records>"]
        xml_data.extend(xml_list)
        xml_data.append("</records></xml>")                         # create formatted xml block

        with open(output_xml,'w',encoding='utf-8') as fx:           # create xml output
                for line in xml_data:
                        if isinstance(line,list):
                                line=[entry.\
                                      encode('utf-8','ignore').\
                                      decode('utf-8') 
                                      for entry in line]            # decode non-ascii characters
                                line='\n'.join(line)
                        fx.write(line)                              # wite xml line
except:
        raise
