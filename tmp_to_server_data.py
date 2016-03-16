
# coding: utf-8

# In[16]:

import os


# In[17]:

def folder_name_mapping(src):
    if src == "alpha":
        return "Alpha"
    if src == "bear":
        return "Bear_Trap"
    if src == "caples":
        return "Caples_Lk"
    if src == "duncan":
        return "Duncan_Pk"
    if src == "dolly":
        return "DollyRice"
    if src == "echo":
        return "Echo_Pk"
    if src == "lost":
        return "Lost_Corner"
    if src == "mt":
        return "Mt_Lincoln"
    if src == "onion":
        return "Onion_Ck"
    if src == "owens":
        return "Owens_Camp"
    if src == "robb":
        return "Robbs_Saddle"
    if src == "schneider":
        return "Schneiders"
    if src == "talbot":
        return "Talbot_Camp"
    if src == "van":
        return "Van_Vleck"
    if src == "test":
        return None


# In[18]:

def transfer_data():
    src_folder_names = os.listdir("tmp/data")
    for src in src_folder_names:
        dst = folder_name_mapping(src)
        if dst is None:
            continue
        src_dir = "tmp/data/" + src + "/"
        dst_dir = "server_data/" + dst
        print "Sync data from", src_dir, "to", dst_dir
        command = "rsync -avz " + src_dir + " " + dst_dir
        os.system(command)


# In[19]:

transfer_data()

