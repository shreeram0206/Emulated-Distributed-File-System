import sys
from datetime import datetime, timezone
import pandas as pd
from lxml import objectify

def xmltotsv(inp_file, out_file):

    root_node = objectify.fromstring(readfile(inp_file))

    dir_dict = {}
    first_parent = root_node.xpath("//INodeSection/inode[name = '']/id")[0]
    dir_dict[first_parent] = []

    dir_dict[first_parent].append(root_node.xpath("//INodeSection/inode[id = '{0}']/name".format(first_parent)))

    children = root_node.xpath("//child")

    for child in children:
        dir_dict[child] = []
        parent = root_node.xpath("//directory[child = '{0}']/parent".format(child))

        if parent[0] in dir_dict.keys():
            dir_dict[child].append(dir_dict[parent[0]])
            parent_name = root_node.xpath("//INodeSection/inode[id = '{0}']/name".format(child))
            dir_dict[child].append(parent_name)
    
    file_info_dict = {"file_paths" : [], "file_mtimes" : [], "file_blockCounts" : [], "file_sizes" : [], "file_permissions" : []}

    for key, value in dir_dict.items():
        value = flatten(value)

        if len(value) == 1:
            dir_dict[key] = '/'
        else:
            listToStr = '/'.join([str(elem) for elem in value])
            dir_dict[key] = listToStr
        file_info_dict["file_paths"].append(dir_dict[key])
            
        mtime = root_node.xpath("//INodeSection/inode[id = '{0}']/mtime".format(key))
        s = datetime.fromtimestamp(mtime[0]/1000, tz = timezone.utc).strftime('%m/%d/%Y %H:%M')
        formatted_s = '/'.join([x.lstrip('0') for x in s.split('/')])
        final_s = ' '.join([x.lstrip('0') for x in formatted_s.split(' ')])
        file_info_dict["file_mtimes"].append(final_s)

        blockCount = root_node.xpath("//INodeSection/inode[id = '{0}']/replication".format(key))
        if len(blockCount) == 0:
            file_info_dict["file_blockCounts"].append(0)
        else:
            file_info_dict["file_blockCounts"].append(blockCount[0])
        fileSize = root_node.xpath("//INodeSection/inode[id = '{0}']/blocks/block/numBytes".format(key))

        if len(blockCount) == 0:
            file_info_dict["file_sizes"].append(0)
        else:
            file_info_dict["file_sizes"].append(fileSize[0])
        
        permission = root_node.xpath("//INodeSection/inode[id = '{0}']/permission".format(key))
        permission = str(permission[0]).split(":")[2][1:]
        fileType = root_node.xpath("//INodeSection/inode[id = '{0}']/type".format(key))
        
        permissions_str = permission_conv(int(permission), fileType[0])
        file_info_dict["file_permissions"].append(permissions_str)

        df_dict = {"Path": file_info_dict["file_paths"], "Modification Time": file_info_dict["file_mtimes"], "BlocksCount": file_info_dict["file_blockCounts"], "FileSize": file_info_dict["file_sizes"], "Permission": file_info_dict["file_permissions"]}
        dataframe = pd.DataFrame(df_dict)
        dataframe.to_csv(out_file, sep="\t", index=False)

def readfile(filepath):
    with open(filepath) as file:
        xml = file.read()
    return xml

def flatten(inpList):
    flatList = []
    for i in inpList:
        if isinstance(i,list): 
            flatList.extend(flatten(i))
        else: 
            flatList.append(i)
    return flatList

def permission_conv(inp, directory):
    if directory == "DIRECTORY":
        result = "d"
    else:
        result = "-"
    value_letters = [(4,"r"),(2,"w"),(1,"x")]
    for digit in [int(n) for n in str(inp)]:
        for value, letter in value_letters:
            if digit >= value:
                result += letter
                digit -= value
            else:
                result += '-'
    return result


if __name__ == "__main__":
    xmltotsv(sys.argv[1], sys.argv[2])

