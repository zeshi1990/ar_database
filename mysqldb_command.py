__author__ = "zeshi"

from mysqldb_level_0 import init_db, populate_data_server, populate_data_sd
from multiprocessing import Pool, cpu_count

# init_db()

site_names = ["Alpha", "Bear_Trap", "Caples_Lk", "DollyRice", "Duncan_Pk", "Echo_Pk", "Mt_Lincoln",\
              "Onion_Ck", "Owens_Camp", "Robbs_Saddle", "Schneiders", "Talbot_Camp", "Van_Vleck"]

pool = Pool(processes=cpu_count())
pool.map(populate_data_server, site_names)
pool.close()
pool.join()