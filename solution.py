from typing import List
import pandas as pd
import json

class Solution:
    def __init__(self, data_file_path: str, protocol_json_path: str):
        self.data_file_path = data_file_path
        self.protocol_json_path = protocol_json_path
       


    def hex_to_ascii(self,hex):
        ascii_str = ""
        for val in hex:
            print(val)
            if not val:
                continue
            byte = int(val,16)
            ascii_str += chr(byte)
        return ascii_str
    
    def convert_txt_to_pd(self):
        col = ['w','Protocol ID' , 'Message Size' , 'Data']

        self.df = pd.DataFrame(columns=col)
        with open(self.data_file_path , 'r') as file:
                flag = 0
                for data in file:
                    line  = data.split(',')
                    # timestamp = line[0]
                    w = line[1]
                    # print(w)
                    protocol = line[2].strip()
                    # print(protocol)
                    size = line[3].strip().split()[0]
                    # print(size)
                    data = line[4]
                    # print(data)
                    # print(byte)
                    # data = line[5]
                
                # Append the extracted data as a new row to the DataFrame
                    self.df = pd.concat([self.df, pd.DataFrame([[ w,protocol, size,data]], columns=col)], ignore_index=True)
        # print(self.df)

        # print(self.df['Message Size'])
        return self.df
    
    def convert_json_to_pd(self):
        with open(self.protocol_json_path,'r') as f:
            json_file = json.load(f)
        # print(self.json_file['protocols'])

        col = ['Protocol ID','fps' , 'dynamic_size']
        self.json_file = pd.DataFrame(columns=col)
        protocols = json_file.get("protocols", {})
        # print(protocols)
        for prot in protocols:
            # print(prot)
            data_prot = protocols.get(prot , {})
            fps = (data_prot['fps'])
            dynamic_size = (data_prot['dynamic_size'])
            self.json_file = pd.concat([self.json_file , pd.DataFrame([[prot , fps , dynamic_size]] , columns=col)] , ignore_index=True)

        return (self.json_file)
    

    # Question 1: What is the version name used in the communication session?
    def q1(self) -> str:
        # print(self.df.iloc)
        #NO Panda, strigh forward txt file.
        #convert to hex in naive way, using base 16 and char
        with open (self.data_file_path, 'r') as data:
            line = data.readline().split(',')
        one_line = (line[4]).split(' ')
        ascii = self.hex_to_ascii(one_line)
        print(ascii)
        

    # Question 2: Which protocols have wrong messages frequency in the session compared to their expected frequency based on FPS?
    def q2(self) -> List[str]:
        #Check how much i have of each protocol
        df = self.convert_txt_to_pd()
        json_file = self.convert_json_to_pd()
        df['count'] = 1

        new_df = df.groupby(['Protocol ID']).count()[['count']].reset_index()
        # new_df = pd.DataFrame(new_df)
        # print(new_df)

        FPS_map = {
            36 : 164,
            18 : 84 ,
            9 : 48,
            1 : 1
        }
        # print(json_file['Protocol ID'][0])
        reulst = {}
        # print(json_file['fps'])
        for idx , row in new_df.iterrows():
            protocol = row['Protocol ID']
            count = row['count']
            # print(protocol)
            for idx1, row1 in json_file.iterrows():
                # print(row1['fps'])
                protocol1 = row1['Protocol ID']
                # print(protocol)
                # print(protocol1)
                if (protocol) == protocol1:
                    # print(row1['fps'])
                    frq = FPS_map[row1['fps']]
                    if frq != count:
                        reulst = {
                            'freq supposed' : frq,
                            'session' : row
                        }
                        
        return reulst

       

    # Question 3: Which protocols are listed as relevant for the version but are missing in the data file?
    def q3(self) -> List[str]:
        with open(self.protocol_json_path,'r') as f:
            json_file = json.load(f)
        relevent_protocol = set((json_file['protocols_by_version']['Version1']['protocols']))
        data = self.convert_txt_to_pd()
        data = data['Protocol ID']
        for ID in data:
            id = ID.strip().lstrip("0x")
            if id in relevent_protocol:
                relevent_protocol.remove(id)
        return relevent_protocol




    # Question 4: Which protocols appear in the data file but are not listed as relevant for the version?
    def q4(self) -> List[str]:
        '''
        The Relevent version is Version1 so i check only there
        '''
        with open(self.protocol_json_path,'r') as f:
            json_file = json.load(f)
        
        data = self.convert_txt_to_pd()
        data = data['Protocol ID']
        result = set()
        relevent_protocol = (json_file['protocols_by_version']['Version1']['protocols'])
        for ID in data:
            id = ID.strip().lstrip("0x")
            if id not in relevent_protocol:
                result.add(ID)
        return result

    # Question 5: Which protocols have at least one message in the session with mismatch between the expected size integer and the actual message content size?
    def q5(self) -> List[str]:
        data = self.convert_txt_to_pd()
        dic = {}
        for idx , row in data.iterrows():
            # print(row['Message Size'])
            data_split = row['Data'].split(' ')
            # print(len(data_split)-1)
            # print(row['Message Size'])
            if (len(data_split)-1) != int(row['Message Size']):
                if row['Protocol ID'] in dic:
                    # print(row['Protocol ID'])
                    dic[row['Protocol ID']] += 1
                    # print('Hi')
                else:
                    dic[row['Protocol ID']] =1

        print(dic)


    # Question 6: Which protocols are marked as non dynamic_size in protocol.json, but appear with inconsistent expected message sizes Integer in the data file?
    def q6(self) -> List[str]:
        pass

sol = Solution('data.txt' , 'protocol.json')
(sol.q5())