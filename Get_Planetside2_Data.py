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
import argparse
import json
import os
import time
import datetime
from typing import List

import networkx as nx

from utils import single_column, multi_column, GiveUpException, fetch_url, SERVICE_ID, DATABASE_PATH, COOL_DOWN, \
    MAX_INACTIVE_DAYS, FRIEND_BATCH_SIZE, fetch_logger, setup_logger, chunks

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


def build_database_tables(table_name: str, archive_connection, output_connection):

    archive_setup_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name}_edge (character_id Primary key, raw TEXT);

    CREATE TABLE IF NOT EXISTS {table_name}_node (character_id Primary key, raw TEXT);

    -- The seed_node table records the set of seed nodes just in case it is
    -- needed for debugging or some unforeseen purpose.
    CREATE TABLE IF NOT EXISTS seed_nodes (name TEXT, seed_nodes TEXT);
    
    CREATE TABLE IF NOT EXISTS {table_name}_problem_character_ids (character_id TEXT);
    """

    archive_connection.executescript(archive_setup_script)

    # the data in two tables Eset and Node which have the format my code
    # actually uses and a third table history stores stat history data in
    # case I want to do something with that later.

    # This database_connection stores the unpacked data in the format used later.
    output_setup_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name}_eset (Source TEXT, Target TEXT, Status TEXT);

    CREATE TABLE IF NOT EXISTS {table_name}_node (character_id PRIMARY KEY, name TEXT, faction TEXT, br INTEGER, 
        outfitTag TEXT, outfitId INTEGER, outfitSize INTEGER, creation_date INTEGER, login_count INTEGER, 
        minutes_played INTEGER, last_login_date INTEGER, kills INTEGER, deaths INTEGER);  

    CREATE TABLE IF NOT EXISTS {table_name}_history (character_id PRIMARY KEY, history TEXT);
    """

    output_connection.executescript(output_setup_script)


def fetch_friend_lists_for_characters(namespace, character_list: List[str], problematic_character_ids: List[int]
                                      ) -> List[dict]:
    """
    Return the list of friend list responses from the server. Also return the list of character ids who couldn't be
    loaded due to errors!
    """

    fetch_logger().info(f"fetch_friend_lists_for_characters {character_list}")
    # Attempt to build a url for this set of characters and handle errors encountered along the way.
    unique_characters = list(set(character_list))

    if len(character_list) > 1:
        character_ids = ",".join(unique_characters)
    else:
        character_ids = str(character_list[0])

    friend_list_results = []

    url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/{namespace}/characters_friend/" \
          f"?character_id={character_ids}&c:resolve=world"

    try:
        decoded = fetch_url(url)
        friend_list_results = decoded["characters_friend_list"]

    except GiveUpException as possible_overload_error:
        # Some characters have errors when you load the friends list. unclear why.
        if len(character_list) > 1:
            fetch_logger().error(f"Unable to load large group of ids: {character_list}")
            fetch_logger().error(str(possible_overload_error))
            for indi_index, individual in enumerate(character_list):
                fetch_logger().info(f"Attempting to run individual {indi_index} ({individual})")

                individual_results = fetch_friend_lists_for_characters(namespace, [individual],
                                                                       problematic_character_ids)
                if len(individual_results) > 0:
                    friend_list_results.extend(individual_results)
                else:
                    fetch_logger().warning(f"Unable to fetch data for player {individual} for whatever reason")

        elif len(character_list) == 1:
            problematic_character_ids.append(character_list)

    except Exception as err:
        fetch_logger().error(f"Unable to fetch friendlist for {character_list} {err} giving up and moving on")
    return friend_list_results


class MainDataCrawler:
    """
    Contains all the methods settings etc needed to build the friend-list graph
    and fetch node attributes of a server in planetside2. The class takes the
    initial of the server to make a graph of when it is first called.
    """

    def __init__(self, server_initial: str, restart: bool, name_overwrite: str = None) -> None:

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

        if name_overwrite is not None:
            self.table_name = f'{self.server_name}_{name_overwrite}'


        setup_logger('DEBUG', self.table_name, self.server_name)

        # The set done contains every node we have already examined
        # including those rejected as too old or otherwise invalid.
        self.done = set()

        # player_friendlist_dict stores the data we have collected the key is the Id of the
        # node while the values are the values of its friends.
        self.player_friendlist_dict = {}

        # The archive_connection database_connection saves the responses from the API so no API call
        # is ever done twice, this is new this version.

        archive_path = os.path.join(DATABASE_PATH, "archive.db")
        fetch_logger().info(archive_path)
        self.archive_connection = sqlite3.connect(archive_path)

        # This database_connection stores the unpacked data in the format used later.

        database_path = os.path.join(DATABASE_PATH, f"{self.server_name}.db")
        self.database_connection = sqlite3.connect(database_path)

        if restart:
            self.clear_results()

        build_database_tables(self.table_name, self.archive_connection, self.database_connection)

    # Gathers edges then gathers information on the nodes.
    def run(self):
        fetch_logger().info("Gathering edges")
        self.get_friendlist_network()
        fetch_logger().info("Gathering node attributes")
        self.get_node_attributes()

    # Crawls the friend list network.
    def get_friendlist_network(self):

        # Get the starting nodes from the leader-boards.
        # If we already have seed nodes for the day simply retrieve them,
        # otherwise gather some.
        existing_seeds = single_column(self.archive_connection, "SELECT name from seed_nodes")

        if self.table_name in existing_seeds:
            fetch_logger().info("We already have fresh player ids for this server from today loading them")
            seed = single_column(
                self.archive_connection,
                f'SELECT seed_nodes from seed_nodes where name = "{self.table_name}"',
            )[0]
            initial_character_ids = seed.split(",")
        else:
            fetch_logger().info("Picking fresh player ids from server leaderboard")
            initial_character_ids = self.leader_board_sample(10)

        # Get raw responses from the server for the nodes we got from the
        # leader board earlier.
        initial_raw_friendlists = self.get_friends(initial_character_ids)

        friends_to_check = self.unpack_friendlists(initial_raw_friendlists)

        while len(friends_to_check) > 0:

            next_raw_friendlists = self.get_friends(friends_to_check)

            friends_to_check = self.unpack_friendlists(next_raw_friendlists)

    def categorize_friends(self, friend):
        """
        Edges status logic as follows:
        Old: Friends who have not logged on since self.tSinceLogin
        cross server: Friends who are on different servers
        Both: both old and cross server (these are so rare...)
        normal: The rest
        """
        character_id = friend.get("character_id", -1)

        if "world_id" not in friend:
            fetch_logger().critical(f"{friend}")
            return "Damaged"

        world_id = friend["world_id"]
        last_online = int(friend.get("last_login_time", -1))

        self.done.add(character_id)
        if last_online > self.tSinceLogin:
            if int(world_id) == int(self.server_id):
                return "normal"
            return "cross server"
        else:
            if int(world_id) == int(self.server_id):
                return "old"
            return "both"


    def unpack_friendlists(self, raw_friendlists: dict) -> list:
        """
        Unpack a bunch of friend lists into the datatables. Returns a list of friends that should be checked

        """
        tstart = time.time()

        insert_edge_query = f"""
        INSERT OR REPLACE INTO {self.table_name}_eset (Source, Target, Status) 
            VALUES (?, ?, ?)
        """

        character_id_queue = set()

        covered_friends = 0
        uncovered_friends = 0
        out_of_scope_friends = 0

        args = []

        for character_id, friend_list in raw_friendlists.items():
            self.done.add(character_id)
            for friend in friend_list:
                # It is at this point that "friend" starts to look like a fake word
                friend_id = friend.get("character_id", -1)

                # If a friends_id is already inside of done we can assume its edge to the current character already
                # exists and move on
                if friend_id in self.done:
                    covered_friends += 1
                    continue

                status = self.categorize_friends(friend)

                # We only want to query "normal" ie not old and same server friends of friends.
                if status == "normal":
                    character_id_queue.add(friend_id)
                    uncovered_friends += 1
                else:
                    out_of_scope_friends += 1

        cursor = self.database_connection.cursor()
        cursor.executemany(insert_edge_query, args)
        self.database_connection.commit()

        fetch_logger().info(f"Unpacked {len(raw_friendlists)} found {covered_friends} characters we already have in "
                            f"the database. {uncovered_friends} characters we need to query and {out_of_scope_friends} "
                            f"friends who are forbidden to this crawler")
        fetch_logger().critical(f"Unpacking took {time.time()-tstart}")
        return list(character_id_queue)

    def get_friends(self, characters_to_query):
        """
        Returns a dictionary where the keys are Ids and values are
        their friends lists
        """
        fetch_logger().info("Gathering friend lists")
        start_time = time.time()
        # Load existing values
        character_friendlist_results = {}
        # All nodes we have a archived friends-list for already.

        archived_friendlist_query = f"""
        SELECT character_id, raw 
            FROM {self.table_name}_edge
            WHERE character_id=?
        """
        unarchived_character_ids = []

        for character_id in characters_to_query:
            archived_friendlist_results = self.archive_connection.execute(archived_friendlist_query,
                                                                          (str(character_id),)).fetchone()
            if archived_friendlist_results is None:
                unarchived_character_ids.append(character_id)
                continue
            self.done.add(character_id)
            decoded = json.loads(archived_friendlist_results[1])
            character_friendlist_results[character_id] = decoded["friend_list"]

        fetch_logger().info(f"We already have {len(character_friendlist_results.keys())} friendlists stored of the "
                            f"{len(characters_to_query)} desired lists!")

        fetch_logger().info(f"Of those {len(unarchived_character_ids)} need to be fetched from the API")

        batched_remaining_nodes = chunks(unarchived_character_ids, FRIEND_BATCH_SIZE)
        total_batches = len(batched_remaining_nodes)

        problematic_character_ids = []

        for batch_number, l in enumerate(batched_remaining_nodes):
            fetch_logger().info(f"[{batch_number} of {total_batches}]")
            results = fetch_friend_lists_for_characters(self.namespace, l, problematic_character_ids)

            for raw_friendlist_record in results:
                # First dump the raw results of the call into a table
                current_char_id = raw_friendlist_record["character_id"]
                serialized_record = json.dumps(raw_friendlist_record)

                self.archive_connection.execute(
                    f"INSERT OR REPLACE into {self.table_name}_edge (character_id, raw) VALUES(?, ?)",
                    (current_char_id, serialized_record),
                )
            self.archive_connection.commit()

            for f in results:
                character_friendlist_results[f["character_id"]] = f["friend_list"]

            for problem in problematic_character_ids:
                problem_id = problem[0]
                problem_report = {'character_id': problem_id}
                serialized_record = json.dumps(problem_report)

                character_friendlist_results[problem_id] = {}

                self.archive_connection.execute(
                    f"INSERT OR REPLACE into {self.table_name}_edge (character_id, raw) VALUES(?, ?)",
                    (problem_id, serialized_record),
                )

            self.archive_connection.commit()
            fetch_logger().info(f"Waiting {COOL_DOWN}")
            time.sleep(COOL_DOWN)

        # Record information on the ids that have been problematic
        for problem_character_id in problematic_character_ids:
            self.archive_connection.execute(
                f"INSERT INTO {self.table_name}problem_character_ids (character_id) VALUES(?)",
                (str(problem_character_id),))
        self.archive_connection.commit()
        fetch_logger().debug(f"get_friends took: {time.time() - start_time}")
        return character_friendlist_results

    # Gathers all node attributes.
    def get_node_attributes(self):
        edges = multi_column(
            self.database_connection,
            f"SELECT Source, Target FROM {self.table_name}_eset WHERE Status=\"normal\"",
        )
        G = nx.Graph()
        G.add_edges_from(edges)
        self.get_character_data(G)
        self.interp_character_data()

    def get_character_data(self, graph):
        nodes = graph.nodes()
        fetch_logger().debug(f'getCharData for {nodes}')

        # Gets character attributes for each found in the friend lists
        archive_id = single_column(
            self.archive_connection, f"SELECT character_id from {self.table_name}_node"
        )
        remaining_nodes = [n for n in nodes if n not in archive_id]
        re_count = len(remaining_nodes)
        fetch_logger().info(
            f"Number of nodes in graph is: {len(nodes)} Number of unarchived nodes is: {re_count}"
        )
        # Break the list up into chunks of 40
        smallLists = chunks(remaining_nodes, 40)
        i = 0
        for character_id_batch in smallLists:
            # After 5000 iterations print the progress %.
            if i % 5000 == 0:
                fetch_logger().info(f"looking up data completion is at {i} ")
            character_ids = ",".join(character_id_batch)
            url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/" \
                  f"{self.namespace}/character/?character_id={character_ids}" \
                  f"&c:resolve=outfit,name,stats,times,stat_history"

            fetch_logger().debug(f'fetching {url}')
            decoded = fetch_url(url)

            fetch_logger().debug(f'{decoded}')

            results = decoded["character_list"]
            for result in results:
                # Unpack the server response and add each to the archive_connection.
                try:
                    self.archive_connection.execute(
                        f"INSERT OR REPLACE into {self.table_name}_node (character_id,raw) VALUES(?,?)",
                        (result["character_id"], json.dumps(result)),
                    )
                except Exception:
                    fetch_logger().info("archive_connection failure")
                    if "error" in str(decoded):
                        fetch_logger().info("Server down")
                        exit(1)
                    else:
                        raise
            self.archive_connection.commit()
            i = i + 40
            # COOL_DOWN second wait seems to be enough to avoid hitting API soft limit
            time.sleep(COOL_DOWN)

    def interp_character_data(self):
        """Unpacks the character data gathered previously,
        reading the raw data from the archive_connection and writing it into the database_connection.
        """
        completed_id = single_column(
            self.database_connection, f"SELECT character_id from {self.table_name}_node"
        )
        results = []
        get_unpacked = f"SELECT character_id, raw from {self.table_name}_node"
        for raw in multi_column(self.archive_connection, get_unpacked):
            if raw[0] not in completed_id:
                results.append(json.loads(raw[1]))
        # Unpack and add it to the snapshots.
        for char_info in results:
            # Basic avatar information.
            character_id = int(char_info.get("character_id", "0000000000000000000"))
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
                    fetch_logger().error(f"Incorrect number of kill stats {death_stats}")
                if len(kill_stats) != 1:
                    fetch_logger().error(f"Incorrect number of kill stats {kill_stats}")
                kills = death_stats[0].get("all_time", -1)
                deaths = kill_stats[0].get("all_time", -1)
            else:
                kills, deaths = -1, -1

            stat_history_schema = f"""
                INSERT or replace INTO {self.table_name}_history (character_id, history) VALUES(?, ?)
            """

            self.database_connection.execute(
                stat_history_schema, (character_id, json.dumps(stats))
            )

            char_data_schema = f"""
                INSERT or replace INTO {self.table_name}_node (
                    character_id, name, faction, br,
                    outfitTag, outfitId, outfitSize,
                    creation_date, login_count, minutes_played, last_login_date,
                    kills, deaths)
                    Values(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            self.database_connection.execute(
                char_data_schema,
                (
                    character_id,
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
            fetch_logger().info(f"Fetching {leaderboard_type} {limit}")
            url = f"http://census.daybreakgames.com/s:{SERVICE_ID}/get/" \
                  f"{self.namespace}/leaderboard/?name={leaderboard_type}" \
                  f"&period=Weekly&world={self.server_name}&c:limit={limit}"

            fetch_logger().info(url)
            decoded = fetch_url(url)
            try:
                decoded_leaderboard = decoded["leaderboard_list"]
            except Exception as err:
                # fetch_logger().error(decoded)
                fetch_logger().error(url)
                fetch_logger().error(f"Failed with {err}")
                raise err
            for characters in decoded_leaderboard:
                character_id = characters.get("character_id")
                if character_id is not None:
                    seed_ids.append(character_id)
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

    def load_raw_edges(self):
        """
        Load in the friends-list from any stored results we may already have
        """
        output_dict = {}
        results = multi_column(self.archive_connection, f"SELECT character_id, raw FROM {self.table_name}_edge")
        for k, val in results:
            output_dict[k] = val

        return output_dict

    def clear_results(self):
        """
        Erase the tables in the database_connection created by this class.
        In theory there is no situation where we would need to remove archived
        values.
        """
        self.database_connection.execute(f"DROP TABLE IF EXISTS {self.table_name}_eset")
        self.database_connection.execute(f"DROP TABLE IF EXISTS {self.table_name}_node")
        self.database_connection.execute(f"DROP TABLE IF EXISTS {self.table_name}_history")

        archive_table_names = [f"{self.table_name}_edge", f"{self.table_name}_node",
                               f"seed_nodes", f"{self.table_name}_problem_character_ids"]

        for name in archive_table_names:
            self.archive_connection.execute(f"DROP TABLE IF EXISTS {name}")




def run_PS4():
    # Crawl the playstation 4 servers.
    # Note that most of these servers have since been merged together.
    for initials in ["graph", "Cr", "L", "S", "Ce", "P"]:
        server_crawler = MainDataCrawler(initials)
        fetch_logger().info(f"Now crawling {server_crawler.server_name}")
        server_crawler.run()


def run_PC():
    # Crawl the specified PC servers.
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument('--target_initials', type=str, nargs='+', default=["E", "C", "M"])
    argument_parser.add_argument('--restart', action='store_true', help='Delete existing data and restart')

    parser_options = argument_parser.parse_args()

    for initials in parser_options.target_initials:
        server_crawler = MainDataCrawler(initials, parser_options.restart, name_overwrite='February_13_2022')

        fetch_logger().info(f"Now crawling {server_crawler.server_name}")
        server_crawler.run()


def sql_columns_to_dicts(table_name, col_name, connection):
    """
    Converts a SQL column to a dictionary keys are the character Id and
    values are whatever is in the column named col_name

    :param table_name: Name of the source table
    :param col_name: Name of the column
    :param connection: Connection to the correct host database
    """
    output_dict = {}
    results = multi_column(connection, f"SELECT character_id, {col_name} FROM {table_name}_node")
    for key, value in results:
        output_dict[str(key)] = value
    return output_dict


def my_set_thing(graph, attribute_name, a_dict):
    """
    Sets all nodes in graph graph with a new attribute named attribute_name using
    values found in a_dict. Networkx has a function that does this but it
    always throws errors if the dict is missing any values in the graph."""

    for node_id in graph.nodes():
        graph.nodes[node_id][attribute_name] = a_dict[node_id]
    return graph


def save_graph_to_graphml(database_name: str, source_table_name: str) -> None:
    """
    Returns the graph and writes it to the desktop for testing in Gephi.


    """
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
    database_path = os.path.join(DATABASE_PATH, database_name)
    database_connection = sqlite3.connect(database_path)
    archive_path = os.path.join(DATABASE_PATH, 'archive.db')
    archive_connection = sqlite3.connect(archive_path)

    edge_raw = multi_column(database_connection, f'SELECT * FROM {source_table_name}_eset where Status="normal"')

    graph = nx.Graph()
    for edge in edge_raw:
        if edge[2] == "normal":
            graph.add_edge(edge[0], edge[1])
            graph[edge[0]][edge[1]]["status"] = edge[2]

    archive_id = single_column(archive_connection, f"Select character_id from {source_table_name}_node")
    remaining_nodes = [n for n in graph.nodes() if n not in archive_id]
    fetch_logger().info(remaining_nodes)
    fetch_logger().info("deleted for being problems")
    graph.remove_nodes_from(remaining_nodes)
    for attr in graphml_attrs:
        graph = my_set_thing(
            graph, attr, sql_columns_to_dicts(source_table_name, attr, database_connection)
        )

    graphml_path = os.path.join("C:", f"{source_table_name}test.graphml")
    nx.write_graphml(graph, graphml_path)

    return graph

if __name__ == "__main__":




    run_PC()
    # save_graph_to_graphml('Connery.db', 'Connery_February_13_2022')
