__all__ = [
    "LiquidSampleAPI",
    "GasSampleAPI",
    "AssemblySampleAPI",
    "UnifiedSampleDataAPI",
    "OldLiquidSampleAPI",
]

import asyncio
import json
import os
import sqlite3
from datetime import datetime
from socket import gethostname
import pandas as pd
from typing import List, Optional, Union, Literal
import aiofiles
import shortuuid
from uuid import UUID


from ..model.sample import (AssemblySample, 
                            LiquidSample, 
                            SampleUnion, 
                            GasSample, 
                            SolidSample,
                            object_to_sample)
from ..helper.file_in_use import file_in_use


class _BaseSampleAPI(object):
    def __init__(self, sampleclass, Serv_class, extra_columns: str):

        self.extra_columns = extra_columns
        self.columns = f"""hlo_version TEXT,
              global_label TEXT NOT NULL,
              sample_type VARCHAR(255) NOT NULL,
              sample_no INTEGER NOT NULL,
              sample_creation_timecode INTEGER NOT NULL,
              sample_position VARCHAR(255),
              machine_name VARCHAR(255) NOT NULL,
              sample_hash VARCHAR(255),
              last_update INTEGER NOT NULL,
              inheritance TEXT,
              status TEXT,
              action_uuid TEXT,
              sample_creation_experiment_uuid VARCHAR(255),
              sample_creation_action_uuid VARCHAR(255),
              server_name VARCHAR(255),
              chemical TEXT,
              mass TEXT,
              supplier TEXT,
              lot_number TEXT,
              source TEXT,
              comment TEXT,
              {self.extra_columns}"""

        self.column_names = self.columns.replace("\n", "").strip().split(",")
        self.column_types = dict()
        self.column_notNULL = dict()
        for i, name in enumerate(self.column_names):
            self.column_names[i] = name.strip().split(" ")[0]
            self.column_types.update({self.column_names[i]:name.strip().split(" ")[1].lower()})
            self.column_notNULL.update({self.column_names[i]:(len(name.strip().split(" "))>2)})
        self.column_count = len(self.column_names)

        self._sampleclass = sampleclass
        self._sample_type = f"{sampleclass.sample_type}_sample"
        self._dbfilename = gethostname() + f"__{self._sample_type}.db"
        self._base = Serv_class
        self._dbfilepath = self._base.db_root
        self._db = os.path.join(self._dbfilepath, self._dbfilename)
        self._con = None
        self._cur = None
        # convert these to json when saving them to the db
        self._jsonkeys = ["chemical", "mass", "supplier", "lot_number", "source", "status", "action_uuid"]
        self.ready = False

    async def _open_db(self):
        while file_in_use(self._db):
            self._base.print_message("db already in use, waiting", info = True)
            await asyncio.sleep(1)
        self._con = sqlite3.connect(self._db)
        self._cur = self._con.cursor()

    def _close_db(self):
        if self._con is not None:
            # commit any changes
            self._con.commit()
            self._con.close()
            self._con = None
            self._cur = None

    def _df_to_sample(self, df):
        """converts db dataframe back to a sample basemodel
        and performs a simply data integrity check"""
        if df.size == 0:
            return None
        sampledict = dict(df.iloc[-1, :])

        for key in self._jsonkeys:
            sampledict.update({key: json.loads(sampledict[key])})

        if sampledict["idx"] != sampledict["sample_no"]:  # integrity check
            raise ValueError(
                f"sampledict['idx'] != sampledict['sample_no']: {sampledict['idx']} != {sampledict['sample_no']}"
            )

        return object_to_sample(sampledict)


    def _create_init_db(self):
        self._cur.execute(
            f"""CREATE TABLE {self._sample_type}(
              idx INTEGER PRIMARY KEY AUTOINCREMENT,
              {self.columns}
              );"""
        )

        self._base.print_message(f"{self._sample_type} table created")
        # commit changes
        self._con.commit()


    async def _append_sample(self, sample):
        await asyncio.sleep(0.001)
        lock = asyncio.Lock()
        async with lock:
            await self._open_db()
            self._cur.execute(f"select count(idx) from {self._sample_type};")
            counts = self._cur.fetchone()[0]
            sample.sample_no = counts + 1
            if sample.machine_name is None:
                sample.machine_name = self._base.hostname
            if sample.server_name is None:
                sample.server_name = self._base.server_name
            # if sample.action_timestamp is None:
            #     atime = datetime.fromtimestamp(datetime.now().timestamp() + self._base.ntp_offset)
            #     sample.action_timestamp = atime.strftime("%Y%m%d.%H%M%S%f")
            if sample.sample_creation_timecode is None:
                sample.sample_creation_timecode = self._base.set_realtime_nowait()
            if sample.last_update is None:
                sample.last_update = self._base.set_realtime_nowait()
            sample.global_label = sample.get_global_label()

            dfdict = sample.dict()
            for key in self._jsonkeys:
                dfdict.update({key: [json.dumps(dfdict[key])]})

            keys_to_deletes = []
            for key,val in dfdict.items():
                if isinstance(val, UUID):
                    dfdict[key] = str(val)

                if key not in self.column_names:
                    self._base.print_message(
                        f"Invalid {self._sample_type} data key '{key}', skipping it.",
                        error=True,
                    )
                    keys_to_deletes.append(key)

            for key in keys_to_deletes:
                del dfdict[key]

            df = pd.DataFrame(data=dfdict)
            df.to_sql(name=self._sample_type, con=self._con, if_exists="append", index=False)

            # now read back the sample and compare and return it
            retdf = pd.read_sql_query(
                f"""select * from {self._sample_type} ORDER BY idx DESC LIMIT 1;""",
                con=self._con,
            )
            self._close_db()
            retsample = self._df_to_sample(retdf)
            return retsample


    async def _key_checks(self, sample):
        return sample


    async def new_sample(self, samples: List[SampleUnion] = None) -> List[SampleUnion]:
        if samples is None:
            samples = []
        while not self.ready:
            self._base.print_message("db not ready", info = True)
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.001)
        ret_samples = []

        for i, sample in enumerate(samples):
            if type(sample) == type(self._sampleclass):
                await asyncio.sleep(0.001)
                sample = await self._key_checks(sample)
                added_sample = await self._append_sample(sample=sample)
                ret_samples.append(added_sample)
            else:
                self._base.print_message(
                    f"wrong sample type {type(sample)}!={self._sample_type}, skipping it",
                    info=True,
                )
                # ret_samples.append(NoneSample())

        return ret_samples


    async def init_db(self):
        lock = asyncio.Lock()
        async with lock:
            await self._open_db()
            # check if table exists
            listOfTables = self._cur.execute(
                f"""SELECT name FROM sqlite_master WHERE type='table'
              AND name='{self._sample_type}';"""
            ).fetchall()

            if listOfTables == []:
                self._base.print_message(
                    f"{self._sample_type} table not found, creating it.",
                    error=True,
                )
                self._create_init_db()
            else:
                self._base.print_message(f"{self._sample_type} table found!")
            self._close_db()
        self._base.print_message(f"'{self._sample_type}' db initialized")
        self.ready = True
        await self.count_samples() # has also a separate lock


    async def count_samples(self) -> int:
        while not self.ready:
            self._base.print_message("db not ready", info = True)
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.001)
        lock = asyncio.Lock()
        async with lock:
            await self._open_db()
            self._cur.execute(f"select count(idx) from {self._sample_type};")
            counts = self._cur.fetchone()[0]
            self._base.print_message(f"sqlite db {self._sample_type} count: {counts}")
            self._close_db()
            return counts


    async def get_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:
        """this will only use the sample_no for local sample, or global_label for external samples
        and fills in the rest from the db and returns the list again.
        We expect to not have mixed sample types here.
        """
        while not self.ready:
            self._base.print_message("db not ready", info = True)
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.001)
        ret_samples = []

        lock = asyncio.Lock()
        async with lock:
            await self._open_db()

            for i, sample in enumerate(samples):
                if sample.sample_no < 0: # get sample from back
                    self._cur.execute(f"select count(idx) from {self._sample_type};")
                    counts = self._cur.fetchone()[0]
                    if counts > abs(sample.sample_no):
                        self._base.print_message(f"getting sample: {self._sample_type} {counts+sample.sample_no+1}")
                        await asyncio.sleep(0.001)
                        retdf = pd.read_sql_query(f"""select * from {self._sample_type} where idx={counts+sample.sample_no+1};""", con=self._con)
                        retsample = self._df_to_sample(retdf)
                        ret_samples.append(retsample)
                    else:
                        self._base.print_message("sample '{sample.sample_no}' does not exist yet", info = True)
                        # ret_samples.append(NoneSample())
                elif sample.sample_no > 0: # get sample from front
                    self._base.print_message(f"getting sample: {self._sample_type} {sample.sample_no}")
                    await asyncio.sleep(0.001)
                    retdf = pd.read_sql_query(f"""select * from {self._sample_type} where idx={sample.sample_no};""", con=self._con)
                    retsample = self._df_to_sample(retdf)
                    ret_samples.append(retsample)
                else:
                    self._base.print_message("zero sample_no is not supported", info = True)
            self._close_db()
        return ret_samples
    
    
    def _update(self, sample_dfdict):
        idx = sample_dfdict["idx"]
        for key, val in sample_dfdict.items():
            if key in self.column_types and key != "idx":
                col_type = self.column_types[key]
                if not (self.column_notNULL[key] and val is None):
                    if col_type in ['integer', 'real']:
                        if val is None:
                            cmd = f'''UPDATE {self._sample_type} SET {key} = NULL WHERE idx = {idx};'''
                        else:
                            cmd = f'''UPDATE {self._sample_type} SET {key} = {val} WHERE idx = {idx};'''
                        self._base.print_message(f"dbcommand: {cmd}")
                        self._cur.execute(cmd)
                    else:
                        if val is not None:
                            val = val.replace('"','""')
                            val = val.replace("'","''")
                            cmd = f'''UPDATE {self._sample_type} SET {key} = "{val}" WHERE idx = {idx};'''
                        else:
                            cmd = f'''UPDATE {self._sample_type} SET {key} = NULL WHERE idx = {idx};'''
                        self._base.print_message(f"dbcommand: {cmd}")
                        self._cur.execute(cmd)

            else:
                if key != "idx":
                    self._base.print_message(f"unknown key '{key}' for updating sample", error=True)


    async def update_sample(self, samples: List[SampleUnion] = []):
        while not self.ready:
            self._base.print_message("db not ready", info = True)
            await asyncio.sleep(0.1)
        
        await asyncio.sleep(0.001)

        lock = asyncio.Lock()
        async with lock:
            await self._open_db()

            for sample in samples:
                self._base.print_message(f"updating sample {self._sample_type} {sample.sample_no}", info = True)
                if sample.global_label is None:
                    self._base.print_message("No global_label. Skipping sample.", info = True)
                    continue

                if sample.sample_no < 1:
                    self._base.print_message("Cannot update sample '{sample.sample_no}'", info = True)
                    continue

                sample = await self._key_checks(sample)

                # get the current info from db so we can perform some checks
                self._base.print_message(f"getting sample {self._sample_type} {sample.sample_no}")
                await asyncio.sleep(0.001)
                retdf = pd.read_sql_query(f"""select * from {self._sample_type} where idx={sample.sample_no};""", con=self._con)
                prev_sample = self._df_to_sample(retdf)
                
                # some safety checks
                if sample.global_label != prev_sample.global_label:
                    self._base.print_message("Cannot update sample '{sample.sample_no}', not the same 'global_label'", error = True)
                    continue

                if sample.sample_type != prev_sample.sample_type:
                    self._base.print_message("Cannot update sample '{sample.sample_no}', not the same 'sample_type'", error = True)
                    continue

                # update the last_update timecode
                sample.last_update = self._base.set_realtime_nowait()

                # update the old sample now
                dfdict = sample.dict()
                for key in self._jsonkeys:
                    # fdict.update({key: [json.dumps(dfdict[key])]})
                    dfdict.update({key: json.dumps(dfdict[key])})
    
                keys_to_deletes = []
                for key in dfdict.keys():
                    if key not in self.column_names:
                        self._base.print_message(
                            f"Invalid {self._sample_type} data key '{key}', skipping it.",
                            error=True,
                        )
                        keys_to_deletes.append(key)
    
                for key in keys_to_deletes:
                    del dfdict[key]
                    
                dfdict.update({"idx": sample.sample_no})
                self._update(dfdict)
                # df = pd.DataFrame(data=dfdict)
                # df.to_sql(name=self._sample_type, con=self._con, if_exists="append", index=False, index_label="idx")
            self._close_db()


class LiquidSampleAPI(_BaseSampleAPI):
    def __init__(self, Serv_class):
        super().__init__(
            sampleclass=LiquidSample(),
            Serv_class=Serv_class,
            extra_columns="volume_ml REAL NOT NULL, ph REAL, dilution_factor REAL NOT NULL",
        )


    async def _key_checks(self, sample):
        if sample.volume_ml is None:
            sample.volume_ml = 0.0
        if sample.dilution_factor is None:
            sample.dilution_factor = 1.0
        return sample


    async def old_jsondb_to_sqlitedb(self):
        old_liquid_sample_db = OldLiquidSampleAPI(self._base)
        counts = await old_liquid_sample_db.count_samples()
        self._base.print_message(f"old db sample count: {counts}")
        for i in range(counts):
            sample = LiquidSample(**{"sample_no":i + 1,"machine_name":gethostname()})
            sample = await old_liquid_sample_db.get_sample(sample)
            sample.server_name = "PAL"
            sample.machine_name = gethostname()
            sample.global_label = sample.get_global_label()
            sample.sample_creation_timecode = 0
            # sample.action_timestamp = "00000000.000000000000"
            await self.new_sample([sample])


class GasSampleAPI(_BaseSampleAPI):
    def __init__(self, Serv_class):
        super().__init__(
            sampleclass=GasSample(),
            Serv_class=Serv_class,
            extra_columns="volume_ml REAL NOT NULL, dilution_factor REAL NOT NULL",
        )


    async def _key_checks(self, sample):
        if sample.volume_ml is None:
            sample.volume_ml = 0.0
        if sample.dilution_factor is None:
            sample.dilution_factor = 1.0
        return sample


class AssemblySampleAPI(_BaseSampleAPI):
    def __init__(self, Serv_class):
        super().__init__(
            sampleclass=AssemblySample(),
            Serv_class=Serv_class,
            extra_columns="parts TEXT",
        )
        self._jsonkeys.append("parts")


class SolidSampleAPI(_BaseSampleAPI):
    def __init__(self, Serv_class):
        super().__init__(
            sampleclass=SolidSample(),
            Serv_class=Serv_class,
            extra_columns="plate_id INTEGER NOT NULL",
        )


    async def new_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:
        self._base.print_message("new_sample is not supported yet for solid sample", error=True)
        await asyncio.sleep(0.001)
        ret_samples = []

        return ret_samples


    async def get_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:

        await asyncio.sleep(0.001)
        ret_samples = []

        for i, sample in enumerate(samples):
            self._base.print_message(f"getting sample {self._sample_type} {sample.sample_no}")
            await asyncio.sleep(0.001)
            if sample.machine_name == "legacy":
                ret_samples.append(
                    SolidSample(
                        plate_id=sample.plate_id,
                        sample_no=sample.sample_no,
                        machine_name="legacy",
                    )
                )

        return ret_samples


    async def update_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:
        self._base.print_message("Update is not supported yet for solid sample", error=True)
        await asyncio.sleep(0.001)


class OldLiquidSampleAPI:
    def __init__(self, Serv_class):
        self._base = Serv_class

        self._dbfile = "liquid_ID_database.csv"
        self._dbfilepath = self._base.db_root
        self.fdb = None
        self.headerlines = 0
        # create folder first if it not exist
        if not os.path.exists(self._dbfilepath):
            os.makedirs(self._dbfilepath)

        if not os.path.exists(os.path.join(self._dbfilepath, self._dbfile)):
            # file does not exists, create file
            f = open(os.path.join(self._dbfilepath, self._dbfile), "w")
            f.close()

        self._base.print_message(
            f"liquid sample no database is: {os.path.join(self._dbfilepath, self._dbfile)}"
        )

    async def _open_db(self, mode):
        if os.path.exists(self._dbfilepath):
            self.fdb = await aiofiles.open(os.path.join(self._dbfilepath, self._dbfile), mode)
            return True
        else:
            return False


    async def _close_db(self):
        await self.fdb.close()


    async def count_samples(self):
        # TODO: faster way?
        _ = await self._open_db("a+")
        counter = 0
        await self.fdb.seek(0)
        async for line in self.fdb:
            counter += 1
        await self._close_db()
        return counter - self.headerlines


    async def new_sample(self, new_sample: LiquidSample):
        async def write_sample_no_jsonfile(filename, datadict):
            """write a separate json file for each new sample_no"""
            self.fjson = await aiofiles.open(os.path.join(self._dbfilepath, filename), "a+")
            await self.fjson.write(json.dumps(datadict))
            await self.fjson.close()

        async def add_line(line):
            if not line.endswith("\n"):
                line += "\n"
            await self.fdb.write(line)

        self._base.print_message(f"new entry dict: {new_sample.dict()}")
        new_sample.sample_no = await self.count_samples() + 1
        self._base.print_message(f"new liquid sample no: {new_sample.sample_no}")
        # dump dict to separate json file
        await write_sample_no_jsonfile(
            f"{new_sample.sample_no:08d}__{new_sample.sample_creation_experiment_uuid}__{new_sample.sample_creation_action_uuid}.json",
            new_sample.dict(),
        )
        # add newid to db csv
        await self._open_db("a+")
        await add_line(f"{new_sample.sample_no},{new_sample.sample_creation_experiment_uuid},{new_sample.sample_creation_action_uuid}")
        await self._close_db()
        return new_sample


    async def get_sample(self, sample: LiquidSample):
        """accepts a liquid sample model with minimum information to find it in the db
        and returns its full information
        """

        async def load_json_file(filename, linenr=1):
            async with aiofiles.open(os.path.join(self._dbfilepath, filename), "r+") as f:
                counter = 0
                retval = ""
                await f.seek(0)
                async for line in f:
                    counter += 1
                    if counter == linenr:
                        retval = line
                retval = json.loads(retval)
                return retval

        async def get_sample_details(sample_no):
            # need to add headerline count
            sample_no = sample_no + self.headerlines
            await self._open_db("r+")
            counter = 0
            retval = ""
            await self.fdb.seek(0)
            async for line in self.fdb:
                counter += 1
                if counter == sample_no:
                    retval = line
                    break
            await self._close_db()
            return retval

        # for now it only checks against local samples
        if sample.machine_name != gethostname():
            self._base.print_message(
                f"can only load from local db, got {sample.machine_name}",
                error=True,
            )
            return None  # return default empty one

        data = await get_sample_details(sample.sample_no)
        if data != "":
            data = data.strip("\n").split(",")
            fileID = int(data[0])
            sample_creation_experiment_uuid = data[1]
            sample_creation_action_uuid = data[2]
            filename = f"{fileID:08d}__{sample_creation_experiment_uuid}__{sample_creation_action_uuid}.json"
            self._base.print_message(f"data json file: {filename}")

            liquid_sample_jsondict = await load_json_file(filename, 1)
            # fix for old db version
            
            comment = "old_comment:"+liquid_sample_jsondict.get("comment","")
            comment += "; plate_id:"+str(liquid_sample_jsondict.get("plate_id",""))
            comment += "; sample_no:"+str(liquid_sample_jsondict.get("sample_no",""))
            liquid_sample_jsondict.update({"comment":comment})
            
            # the action time was something different and doesn't map
            # to any new fields
            # del liquid_sample_jsondict["action_time"]
            # liquid_sample_jsondict.update({"action_timestamp":liquid_sample_jsondict.get("action_time","00000000.000000000000")})

            volume_mL = liquid_sample_jsondict.get("volume_mL",0.0)
            liquid_sample_jsondict.update({"volume_ml":volume_mL})
            source = liquid_sample_jsondict.get("source",[])
            new_source = []
            for src in source:
                if src != "":
                    new_source.append(f"{gethostname()}__liquid__{src}")
                
            liquid_sample_jsondict.update({"source":new_source})

            if "id" in liquid_sample_jsondict:  # old v1
                # liquid_sample_jsondict["plate_sample_no"] = liquid_sample_jsondict["sample_no"]
                liquid_sample_jsondict["sample_no"] = liquid_sample_jsondict["id"]
                del liquid_sample_jsondict["id"]
                if "plate_id" in liquid_sample_jsondict:
                    del liquid_sample_jsondict["plate_id"]

            if "sample_id" in liquid_sample_jsondict:
                liquid_sample_jsondict["sample_no"] = liquid_sample_jsondict["sample_id"]
                del liquid_sample_jsondict["sample_id"]

            if "AUID" in liquid_sample_jsondict:
                liquid_sample_jsondict["sample_creation_action_uuid"] = shortuuid.decode(liquid_sample_jsondict['AUID'])
                del liquid_sample_jsondict["AUID"]

            if "DUID" in liquid_sample_jsondict:
                liquid_sample_jsondict["sample_creation_experiment_uuid"] = shortuuid.decode(liquid_sample_jsondict['DUID'])
                del liquid_sample_jsondict["DUID"]

            ret_liquid_sample = LiquidSample(**liquid_sample_jsondict)
            self._base.print_message(f"data json content: {ret_liquid_sample.dict()}")

            return ret_liquid_sample
        else:
            return None  # will be default empty one


class UnifiedSampleDataAPI:
    def __init__(self, Serv_class):
        self._base = Serv_class
        self._dbfilepath = self._base.db_root

        self.solidAPI = SolidSampleAPI(self._base)
        self.liquidAPI = LiquidSampleAPI(self._base)
        self.gasAPI = GasSampleAPI(self._base)
        self.assemblyAPI = AssemblySampleAPI(self._base)
        self.ready = False

    async def init_db(self) -> None:
        await self.solidAPI.init_db()
        await self.liquidAPI.init_db()
        await self.gasAPI.init_db()
        await self.assemblyAPI.init_db()
        self._base.print_message("unified db initialized")
        self.ready = True


    async def new_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:
        retval = []

        for sample in samples:
            if isinstance(sample, LiquidSample):
                tmp = await self.liquidAPI.new_sample(samples=[sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, SolidSample):
                tmp = await self.solidAPI.new_sample(samples=[sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, GasSample):
                tmp = await self.gasAPI.new_sample(samples=[sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, AssemblySample):
                tmp = await self.assemblyAPI.new_sample(samples=[sample])
                for t in tmp: retval.append(t)

        return retval


    async def get_sample(self, samples: List[SampleUnion] = []) -> List[SampleUnion]:
        """this will only use the sample_no for local sample, or global_label for external samples
        and fills in the rest from the db and returns the list again.
        We expect to not have mixed sample types here.
        """
        retval = []

        for sample in samples:
            self._base.print_message(f"retrieving sample {sample}")
            self._base.print_message(f"retrieving sample {sample.get_global_label()}")
            if isinstance(sample, LiquidSample):
                tmp = await self.liquidAPI.get_sample([sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, SolidSample):
                tmp = await self.solidAPI.get_sample([sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, GasSample):
                tmp = await self.gasAPI.get_sample([sample])
                for t in tmp: retval.append(t)
            elif isinstance(sample, AssemblySample):
                tmp = await self.assemblyAPI.get_sample([sample])
                for t in tmp: retval.append(t)
            else:
                self._base.print_message(
                    f"validation error, type '{type(sample)}' is not a valid sample model",
                    error=True,
                )

        return retval

    
    async def update_sample(self, samples: List[SampleUnion] = []) -> None:
        for sample in samples:
            self._base.print_message(f"updating sample: {sample.global_label}", info=True)
            if isinstance(sample, LiquidSample):
                await self.liquidAPI.update_sample([sample])
            elif isinstance(sample, SolidSample):
                await self.solidAPI.update_sample([sample])
            elif isinstance(sample, GasSample):
                await self.gasAPI.update_sample([sample])
            elif isinstance(sample, AssemblySample):
                # update also the parts
                await self.update_sample(sample.parts)
                await self.assemblyAPI.update_sample([sample])
            else:
                self._base.print_message(
                    f"validation error, type '{type(sample)}' is not a valid sample model",
                    error=True,
                )
