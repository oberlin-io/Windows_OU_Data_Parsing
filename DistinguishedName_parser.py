import os
import pandas as pd
import re

# Instead of merging an OU column, encode so columns like "OU=This OU"
#

INPUTPATH=''
OUTPUTPATH=''

filepath=os.path.join(INPUTPATH, 'Get_ADComputer.csv')
Get_AD_df=pd.read_csv(filepath, skiprows=1)

#Get_AD_df.index=Get_AD_df.SID

drop=[
    #'DistinguishedName',
    #'DNSHostName',
    'Enabled',
    #'IPv4Address',
    'Name',
    'ObjectClass',
    'ObjectGUID',
    'SamAccountName',
    #'SID',
    'UserPrincipalName',
]
Get_AD_df.drop(columns=drop, inplace=True)

keys=list()
values=list()
SIDs=list()
def parse_DistinguishedName(record, keys, values, SIDs):
    DistinguishedName=record['DistinguishedName']
    dn_split=DistinguishedName.split('=')
    SIDs.append(record['SID'])
    keys.append(dn_split[0])
    #values=list()
    for i in dn_split[1:-1]:
        SIDs.append(record['SID'])
        comma_find=re.findall(',', i)
        if len(comma_find) == 1:
            values.append(''.join(i.split(',')[:-1]))
            keys.append(i.split(',')[-1])
        elif len(comma_find) > 1:
            values.append(','.join(i.split(',')[:-1]))
            keys.append(i.split(',')[-1])
    values.append(dn_split[-1])
    #return keys, values

Get_AD_df.apply(lambda x: parse_DistinguishedName(x, keys, values, SIDs), axis=1)

dn_df=pd.DataFrame({
    'DistinguishedName_key': keys,
    'DistinguishedName_value': values,
    'SID': SIDs,
    })

#dn_df=dn_df[dn_df.DistinguishedName_key!='CN']
ou_df=dn_df[dn_df.DistinguishedName_key=='OU']
ou_df=ou_df.rename(columns={'DistinguishedName_value': 'OU'}).drop(columns='DistinguishedName_key')

#dc_df=dn_df[dn_df.DistinguishedName_key=='DC']
#dc_df=dc_df.rename(columns={'DistinguishedName_value': 'DC'}).drop(columns='DistinguishedName_key')

#for df in [ou_df, dc_df]:
Get_AD_df=pd.merge(Get_AD_df, ou_df, how='left', left_on='SID', right_on='SID')

filepath=os.path.join(OUTPUTPATH, 'Assets by OU.csv')
Get_AD_df.to_csv(filepath, index=False)
