"""
Updated 2022 to account for census API changes. And to account for the fact I know how to code now. :P

This code builds a graph of the frendslist of active characters in the MMO
planetside 2 by using the census API.

Information on the API is found here at http://census.daybreakgames.com/, you may request
a Service ID of your own on the page they seem very

The official limit on query's is no more than 100 in 1 minute, or you risk
having your connection terminated or being banned outright from the API.
"""
import sqlite3
import json
import os
import time
import datetime
from typing import List

import networkx as nx

from utils import single_column, multi_column, GiveUpException, fetch_url, logger, SERVICE_ID, DATABASE_PATH, COOL_DOWN, \
    MAX_INACTIVE_DAYS, FRIEND_BATCH_SIZE


"""
The Playstation 4 has 2 separate name spaces one for Europe and one for
north America the correct name space must be used for each servers, those
name spaces as well as the world Id numbers are contained in the following
dictionaries keyed by the initials of the server you want to investigate.

US: ps2ps4us:v2
    Servers
    Palos world_id = 1001 Initial: P
    Genudine world_id = 1000 Initial: G

EU: ps2ps4eu:v2
    Servers
    Ceres world_id = 2000 Initial: Ce
    Lithcorp world_id = 2001 Initial: L

The PC name space is ps2:v2
    Servers
    Connery world_id = 1 Initial: C
    Emerald world_id = 17 Initial: E
    Miller world_id = 10 Initial: M
"""
namespace_dict = {
    "P": "ps2ps4us:v2",
    "G": "ps2ps4us:v2",
    "S": "ps2ps4us:v2",
    "Cr": "ps2ps4us:v2",
    "Ce": "ps2ps4eu:v2",
    "L": "ps2ps4eu:v2",
    "C": "ps2:v2",
    "E": "ps2:v2",
    "M": "ps2:v2",
}
server_id_dict = {
    "G": "1000",
    "P": "1001",
    "Cr": "1002",
    "S": "1003",
    "Ce": "2000",
    "L": "2001",
    "C": "1",
    "E": "17",
    "M": "10",
}

server_name_dict = {
    "P": "Palos",
    "G": "Genudine",
    "Ce": "Ceres",
    "S": "Searhus",
    "L": "Lithcorp",
    "C": "Connery",
    "E": "Emerald",
    "M": "Miller",
    "Cr": "Crux",
}

stat_history_schema = """
    INSERT or replace INTO {}_history (Id, history) VALUES(?, ?)
    """

char_data_schema = """
    INSERT or replace INTO {}_node (
        Id, name, faction, br,
        outfitTag, outfitId, outfitSize,
        creation_date, login_count, minutes_played, last_login_date,
        kills, deaths)
        Values(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """


def build_database_tables(table_name: str, archive_connection, output_connection):

    archive_setup_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name}_edge (Id Primary key,raw TEXT);

    CREATE TABLE IF NOT EXISTS {table_name}_node (Id Primary key, raw TEXT);

    -- The seed_node table records the set of seed nodes just in case it is
    -- needed for debugging or some unforeseen purpose.
    CREATE TABLE IF NOT EXISTS seed_nodes (name TEXT, seed_nodes TEXT);
    
    CREATE TABLE IF NOT EXISTS {table_name}problem_character_ids (character_id TEXT);
    """

    archive_connection.executescript(archive_setup_script)

    # the data in two tables Eset and Node which have the format my code
    # actually uses and a third table history stores stat history data in
    # case I want to do something with that later.

    # This database_connection stores the unpacked data in the format used later.
    output_setup_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name}_eset (Source TEXT, Target TEXT, Status TEXT);

    CREATE TABLE IF NOT EXISTS {table_name}_node (Id PRIMARY KEY, name TEXT, faction TEXT, br INTEGER, 
        outfitTag TEXT, outfitId INTEGER, outfitSize INTEGER, creation_date INTEGER, login_count INTEGER, 
        minutes_played INTEGER, last_login_date INTEGER, kills INTEGER, deaths INTEGER);  

    CREATE TABLE IF NOT EXISTS {table_name}_history (Id PRIMARY KEY, history TEXT);
    """

    output_connection.executescript(output_setup_script)


def fetch_friend_lists_for_characters(namespace, character_list: List[str], problematic_character_ids: List[int]
                                      ) -> List[dict]:
    """
    Return the list of friend list responses from the server. Also return the list of character ids who couldn't be
    loaded due to errors!
    """

    logger.info(f"fetch_friend_lists_for_characters {character_list}")
    # Attempt to build a url for this set of characters and handle errors encountered along the way.
    unique_characters = list(set(character_list))

    if len(character_list) > 1:
        character_ids = ",".join(unique_characters)
    else:
        character_ids = str(character_list[0])

    friend_list_results = []

    url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/{namespace}/characters_friend/" \
          f"?character_id={character_ids}"
    try:
        decoded = fetch_url(url)
        logger.debug(decoded)
        friend_list_results = decoded["characters_friend_list"]

    except GiveUpException as possible_overload_error:
        # Some characters have errors when you load the friends list. unclear why.
        if len(character_list) > 1:
            logger.error(f"Unable to load large group of ids: {character_list}")
            logger.error(str(possible_overload_error))
            for indi_index, individual in enumerate(character_list):
                logger.info(f"Attempting to run individual {indi_index} ({individual})")

                individual_results = fetch_friend_lists_for_characters(namespace, [individual],
                                                                       problematic_character_ids)
                if len(individual_results) > 0:
                    friend_list_results.extend(individual_results)
                else:
                    logger.warning(f"Unable to fetch data for player {individual} for whatever reason")

        elif len(character_list) == 1:
            problematic_character_ids.append(character_list)

    except Exception as err:

        logger.error(f"Unable to fetch friendlist for {character_list} {err}"
                     f"giving up and moving on")

    return friend_list_results


class MainDataCrawler:
    """
    Contains all the methods settings etc needed to build the friend-list graph
    and fetch node attributes of a server in planetside2. The class takes the
    initial of the server to make a graph of when it is first called.
    """

    def __init__(self, server_initial):

        """
        This limits strain on the database_connection by restricting our attention to only
        those nodes active in last MAX_INACTIVE_DAYS days."""
        self.current_time = time.mktime(time.localtime())
        current_datetime = datetime.datetime.now()
        self.tSinceLogin = self.current_time - MAX_INACTIVE_DAYS * 24 * 3600
        # Using the dictionaries above get the name space server Id and
        # server name of the server we wish to crawl.
        # Remember you can change these class variables if needed.
        self.namespace = namespace_dict[server_initial]
        self.server_id = server_id_dict[server_initial]
        self.server_name = server_name_dict[server_initial]

        self.table_name = f'{self.server_name}_{current_datetime.strftime("%B_%d_%Y")}'

        # The set done contains every node we have already examined
        # including those rejected as too old or otherwise invalid.
        self.done = set()

        # player_friendlist_dict stores the data we have collected the key is the Id of the
        # node while the values are the values of its friends.
        self.player_friendlist_dict = {}

        # The archive_connection database_connection saves the responses from the API so no API call
        # is ever done twice, this is new this version.

        archive_path = os.path.join(DATABASE_PATH, "archive.db")
        logger.info(archive_path)
        self.archive_connection = sqlite3.connect(archive_path)

        # This database_connection stores the unpacked data in the format used later.

        database_path = os.path.join(DATABASE_PATH, f"{self.server_name}.db")
        self.database_connection = sqlite3.connect(database_path)

        build_database_tables(self.table_name, self.archive_connection, self.database_connection)

        # Get the starting nodes from the leader-boards.
        # If we already have seed nodes for the day simply retrieve them,
        # otherwise gather some.
        existing_seeds = single_column(self.archive_connection, "SELECT name from seed_nodes")

        if self.table_name in existing_seeds:
            logger.info("We already have fresh player ids for this server from today loading them")
            seed = single_column(
                self.archive_connection,
                f"SELECT seed_nodes from seed_nodes where name = \"{self.table_name}\"",
            )[0]
            self.listToCheck = seed.split(",")
        else:
            logger.info("Picking fresh player ids from server leaderboard")
            self.listToCheck = self.leader_board_sample(75)

    # Gathers edges then gathers information on the nodes.
    def run(self):
        logger.info("Gathering edges")
        self.get_friendlist_network()
        logger.info("Gathering node attributes")
        self.get_node_attributes()

    # Crawls the friend list network.
    def get_friendlist_network(self):
        # Get raw responses from the server for the nodes we got from the
        # leader board earlier.
        self.get_friends(self.listToCheck)
        while len(self.listToCheck) > 0:
            self.expand_graph()
            logger.info("Nodes left to check %s" % str(len(self.listToCheck)))

    def categorize_friends(self, friend):
        """
        Edges status logic as follows:
        Old: Friends who have not logged on since self.tSinceLogin
        cross server: Friends who are on different servers
        Both: both old and cross server (these are so rare...)
        normal: The rest
        """
        Id = friend.get("character_id", -1)
        world_id = friend.get("world_id", -1)
        last_online = int(friend.get("last_login_time", -1))

        self.done.add(Id)
        if last_online > self.tSinceLogin:
            if int(world_id) == int(self.server_id):
                return "normal"
            else:
                return "cross server"
        else:
            if int(world_id) == int(self.server_id):
                return "old"
            else:
                return "both"
        return "error"

    # Updates the list of nodes to Check and manages what nodes to ask the API
    # about, also saves data to the SQL database_connection.
    def expand_graph(self):
        Queue = set()
        Check = set()
        # Get the list of nodes we have server responses for.
        valid = list(self.player_friendlist_dict.keys())
        for i in self.listToCheck:
            # If i is in the id_dict we unpack its friends-list and go through
            # each of its friends to determine if we traverse them as well.
            # Other wise we add it to the Queue.
            if i in valid:
                friend_list = self.player_friendlist_dict[i]
                self.done.add(i)
                for friend in friend_list:

                    Id = friend.get("character_id", -1)

                    if Id not in self.done:

                        status = self.categorize_friends(friend)
                        # Add normal characters to the Queue.
                        if status == "normal":
                            Queue.add(Id)

                        # Inserts the information into the Eset.
                        self.database_connection.execute(
                            f"INSERT OR REPLACE INTO {self.table_name}_eset "
                            "(Source,Target,Status) Values(?,?,?)",
                            (i, Id, status),
                        )
            else:
                Check.add(i)
                Queue.add(i)
        Check = list(Check)
        Queue = list(Queue)
        logger.info("Queue len %s" % len(Queue))
        logger.info("Query len %s" % len(Check))
        # If there are nodes in check, fetch repeat.
        if len(Check) > 0:
            self.player_friendlist_dict = self.get_friends(Check)
        # Then add those nodes to the list of nodes to check.
        self.listToCheck = Queue
        self.database_connection.commit()

    def get_friends(self, to_check):
        """
        Returns a dictionary where the keys are Ids and values are
        their friends lists
        """
        logger.info("Gathering friend lists")
        start_time = time.mktime(time.localtime())
        # Load existing values
        idDict = {}
        # All nodes we have a archived friends-list for already.
        archive_id = single_column(
            self.archive_connection, "SELECT Id FROM " + self.table_name + "Edge"
        )
        # List of all nodes for which no archived value exists.
        remaining_nodes = [n for n in to_check if n not in archive_id]

        batched_remaining_nodes = self.chunks(remaining_nodes, FRIEND_BATCH_SIZE)
        total_batches = len(batched_remaining_nodes)

        problematic_character_ids = []

        for batch_number, l in enumerate(batched_remaining_nodes):
            logger.info(f"[{batch_number} of {total_batches}]")
            results = fetch_friend_lists_for_characters(self.namespace, l, problematic_character_ids)
            for raw_friendlist_record in results:
                # First dump the raw results of the call into a table
                current_char_id = raw_friendlist_record["character_id"]
                try:
                    serialized_record = json.dumps(raw_friendlist_record)

                except TypeError:
                    logger.error("Unable to serialize raw_friendlist_record")
                    logger.error(str(raw_friendlist_record)[:100])
                    raise

                try:
                    self.archive_connection.execute(
                        f"INSERT OR REPLACE into {self.table_name}_edge (Id, raw) VALUES(?, ?)",
                        (current_char_id, serialized_record),
                    )

                except Exception:
                    logger.error("archive_connection failure")
                    # logger.info(f"{current_char_id} {serialized_record}")
                    raise
            for f in results:
                idDict[f["character_id"]] = f["friend_list"]

            for problem in problematic_character_ids:

                problem_id = problem[0]
                # raise Exception(f"{problem_id}")

                problem_report = {'character_id': problem_id}
                serialized_record = json.dumps(problem_report)

                idDict[problem_id] = {}

                self.archive_connection.execute(
                    f"INSERT OR REPLACE into {self.table_name}_edge (Id, raw) VALUES(?, ?)",
                    (problem_id, serialized_record),
                )

            self.archive_connection.commit()
            time.sleep(COOL_DOWN)

        # Record information on the ids that have been problematic
        for problem_character_id in problematic_character_ids:
            self.archive_connection.execute(
                f"INSERT INTO {self.table_name}problem_character_ids (character_id) VALUES(?)",
                (str(problem_character_id),))
        self.archive_connection.commit()

        # Load in the friends-list from any stored results we may already have
        archived_friends_lists = self.sql_columns_to_dicts("Edge", "raw", self.archive_connection)
        for l in [i for i in to_check if i not in remaining_nodes]:
            f = json.loads(archived_friends_lists[l])
            try:
                idDict[f["character_id"]] = f["friend_list"]
            except Exception:
                logger.info("get friends error")
                logger.info(l)
                logger.info(f)
                raise
        logger.info(
            "Elapsed time: %s" % str((time.mktime(time.localtime()) - start_time))
        )
        return idDict

    # Gathers all node attributes.
    def get_node_attributes(self):
        edges = multi_column(
            self.database_connection,
            f"SELECT Source,Target FROM {self.table_name}_eset WHERE Status=\"normal\"",
        )
        G = nx.Graph()
        G.add_edges_from(edges)
        self.get_character_data(G.nodes())
        self.interp_character_data()

    def get_character_data(self, nodes):

        logger.debug(f'getCharData for {nodes}')

        # Gets character attributes for each found in the friend lists
        archive_id = single_column(
            self.archive_connection, f"SELECT Id from {self.table_name}_node"
        )
        remaining_nodes = [n for n in nodes if n not in archive_id]
        re_count = len(remaining_nodes)
        logger.info(
            f"Number of nodes in graph is: {len(nodes)} Number of unarchived nodes is: {re_count}"
        )
        # Break the list up into chunks of 40
        smallLists = self.chunks(remaining_nodes, 40)
        i = 0
        for l in smallLists:
            # After 5000 iterations print the progress %.
            if i % 5000 == 0:
                logger.info(f"looking up data completion is at {i} ")
            character_ids = ",".join(l)
            url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/" \
                  f"{self.namespace}/character/?character_id={character_ids}" \
                  f"&c:resolve=outfit,name,stats,times,stat_history"

            logger.debug(f'fetching {url}')
            decoded = fetch_url(url)

            logger.debug(f'{decoded}')

            results = decoded["character_list"]
            for x in results:
                # Unpack the server response and add each to the archive_connection.
                try:
                    self.archive_connection.execute(
                        f"INSERT OR REPLACE into {self.table_name}_node (Id,raw) VALUES(?,?)",
                        (x["character_id"], json.dumps(x)),
                    )
                except Exception:
                    logger.info("archive_connection failure")
                    if "error" in str(decoded):
                        logger.info("Server down")
                        exit
                    else:
                        raise
            self.archive_connection.commit()
            i = i + 40
            # 2 second wait seems to be enough to avoid hitting API soft limit
            time.sleep(COOL_DOWN)

    def interp_character_data(self):
        """Unpacks the character data gathered previously,
        reading the raw data from the archive_connection and writing it into the database_connection.
        """
        completed_id = single_column(
            self.database_connection, "SELECT Id from " + self.table_name + "Node"
        )
        results = []
        get_unpacked = "SELECT Id,raw from {}_node".format(self.table_name)
        for raw in multi_column(self.archive_connection, get_unpacked):
            if raw[0] not in completed_id:
                results.append(json.loads(raw[1]))
        # Unpack and add it to the snapshots.
        for char_info in results:
            # Basic avatar information.
            Id = int(char_info.get("character_id", "0000000000000000000"))
            name = char_info["name"].get("first", "not available")
            faction_id = char_info.get("faction_id", -1)
            faction = {"1": "VS", "2": "NC", "3": "TR"}.get(
                faction_id, "has no faction"
            )
            br = char_info.get("battle_rank", {"value": "-1"})["value"]
            # Time data:
            tInfo = char_info.get("times")
            creation_date = tInfo.get("creation", "0")
            login_count = tInfo.get("login_count", "0")
            minutes_played = tInfo.get("minutes_played", "0")
            last_login_date = tInfo.get("last_login", "0")
            # Outfit data:
            o = char_info.get("outfit", {"placeholder": "error2"})
            outfitTag = o.get("alias", -1)
            # outfitName = o.get('name', 'not available')
            outfitId = o.get("outfit_id", -1)
            outfitSize = o.get("member_count", -1)
            # Stat history is formatted differently, it returns a list
            # of stats of dictionaries containing the stat history:
            stats = char_info.get("stats", {"placeholder": "error2"}).get(
                "stat_history"
            )
            if type(stats) == list:
                death_stats = [sd for sd in stats if sd["stat_name"] == "deaths"]
                kill_stats = [sd for sd in stats if sd["stat_name"] == "kills"]

                if len(death_stats) != 1:
                    logger.error(f"Incorrect number of kill stats {death_stats}")
                if len(kill_stats) != 1:
                    logger.error(f"Incorrect number of kill stats {kill_stats}")
                kills = death_stats[0].get("all_time", -1)
                deaths = kill_stats[0].get("all_time", -1)
            else:
                kills, deaths = -1, -1
            self.database_connection.execute(
                stat_history_schema.format(self.table_name), (Id, json.dumps(stats))
            )

            self.database_connection.execute(
                char_data_schema.format(self.table_name),
                (
                    Id,
                    name,
                    faction,
                    br,
                    outfitTag,
                    outfitId,
                    outfitSize,
                    creation_date,
                    login_count,
                    minutes_played,
                    last_login_date,
                    kills,
                    deaths,
                ),
            )

        self.database_connection.commit()

    def leader_board_sample(self, limit=50):
        """
        I have used more than one method to get the initial list of
        character Ids. The first version simply used the id's of characters
        I was knew of. This new version is a bit less biased It gathers the
        players who were in the top limit places on the current leader-board
        for all areas of the leader-board available.
        Note that all leader-board stats are strongly correlated.
        """

        seed_ids = []
        for leaderboard_type in ["Kills", "Time", "Deaths", "Score"]:
            url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/" \
                  f"{self.namespace}/leaderboard/?name={leaderboard_type}" \
                  f"&period=Forever&world={self.server_name}&c:limit={limit}"

            logger.info(url)
            decoded = fetch_url(url)
            try:
                H = decoded["leaderboard_list"]
            except Exception as err:
                #
                # logger.error(decoded)
                logger.error(url)
                logger.error(f"Failed with {err}")
            for characters in H:
                Id = characters.get("character_id")
                if Id is not None:
                    seed_ids.append(Id)
        unique = list(set(seed_ids))
        # Record the starting nodes for debugging. The busy_timeout prevents
        # a issue where sqlite3 was not waiting long enough.
        # It probably isn't needed but....
        self.archive_connection.execute("PRAGMA busy_timeout = 30000")
        self.archive_connection.execute(
            "INSERT INTO seed_nodes (name,seed_nodes) VALUES(?,?)",
            (self.table_name, ",".join(unique)),
        )
        return seed_ids

    # Returns the graph and writes it to the desktop for testing in Gephi.
    def save_graph_to_graphml(self, xtend=""):
        graphml_attrs = [
            "name",
            "faction",
            "br",
            "outfitTag",
            "outfitId",
            "outfitSize",
            "creation_date",
            "login_count",
            "minutes_played",
            "last_login_date",
        ]

        edge_raw = multi_column(self.database_connection, f'SELECT * FROM {self.table_name}_eset where Status="normal"')
        
        graph = nx.Graph()
        for edge in edge_raw:
            if edge[2] == "normal":
                graph.add_edge(edge[0], edge[1])
                graph[edge[0]][edge[1]]["status"] = edge[2]
        
        archive_id = single_column(
            self.archive_connection, f"Select Id from {self.table_name}_node"
        )
        remaining_nodes = [n for n in graph.nodes() if n not in archive_id]
        logger.info(remaining_nodes)
        logger.info("deleted for being problems")
        graph.remove_nodes_from(remaining_nodes)
        for attr in graphml_attrs:
            try:
                graph = self.my_set_thing(
                    graph, attr, self.sql_columns_to_dicts("Node", attr, self.database_connection)
                )
            except Exception:
                logger.info("failure on %s" % attr)
                raise
        graphml_path = os.path.join("C:", f"{self.table_name}test.graphml")
        nx.write_graphml(graph, graphml_path)

        return graph

    def my_set_thing(self, G, attri_name, a_dict):
        """
        Sets all nodes in graph G with a new attribute named attri_name using
        values found in a_dict. Networkx has a function that does this but it
        always throws errors if the dict is missing any values in the graph."""

        for n in G.nodes():
            G.node[n][attri_name] = a_dict[n]
        return G

    def sql_columns_to_dicts(self, table_type, col_name, connection):
        """
        Converts a SQL column to a dictionary keys are the character Id and
        values are whatever is in the column named col_name

        Args:
            table is either Node or Edge
        """
        d = {}
        val = multi_column(
            connection,
            f"SELECT Id,{col_name} FROM {self.table_name}{table_type}"
        )
        for i in val:
            d[str(i[0])] = i[1]
        return d

    def clear_results(self):
        """
        Erase the tables in the database_connection created by this class.
        In theory there is no situation where we would need to remove archived
        values.
        """
        self.database_connection.execute(f"DROP TABLE {self.table_name}_eset")
        self.database_connection.execute(f"DROP TABLE {self.table_name}_node")
        self.database_connection.execute(f"DROP TABLE {self.table_name}_history")

    def chunks(self, alist, n):
        """Breaks a list into a list of length n list."""
        outList = []
        for i in range(0, len(alist), n):
            outList.append(alist[i: i + n])
        return outList


def run_PS4():
    # Crawl the playstation 4 servers.
    # Note that most of these servers have since been merged together.
    for initials in ["G", "Cr", "L", "S", "Ce", "P"]:
        server_crawler = MainDataCrawler(initials)
        logger.info(f"Now crawling {server_crawler.server_name}")
        server_crawler.run()


def run_PC():
    # Crawl the 3 PC servers.
    # for initials in ["E", "C", "M"]:
    for initials in ["C"]:
        server_crawler = MainDataCrawler(initials)
        logger.info(f"Now crawling {server_crawler.server_name}")
        server_crawler.run()


if __name__ == "__main__":
    run_PC()
