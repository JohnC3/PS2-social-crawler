'''
Rewritten for readability but it keeps it original loop structure, it
would be dishonest to change how I did it for the original data.

Read through everything carefully the structure is very odd.

This code builds a graph of the frendslist of active characters in the MMO
planetside 2 by using the census API.

Information on the API is found here at http://census.soe.com/, you may request
a Service ID of your own on the page they seem very

The official limit on query's is no more than 100 in 1 minute, or you risk
having your connection terminated or being banned outright from the API.
'''


import logging
import sqlite3
import json
import os
import sys
import urllib.request
import time
import datetime
import networkx as nx



root = logging.getLogger(__name__)
root.setLevel(logging.INFO)

console_handle = logging.StreamHandler(sys.stdout)
console_handle.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handle.setFormatter(formatter)
root.addHandler(console_handle)

current_date = datetime.datetime.now().strftime("%B%d")
log_path = os.path.join(
    'C:\\Users\\jscle\\Desktop\\PS2-social-crawler',
    '%s_logs.log' % current_date)
with open(log_path, 'w') as f:
    f.write('')
file_handle = logging.FileHandler(log_path, 'w')

file_handle.setLevel(logging.INFO)
file_handle.setFormatter(formatter)
root.addHandler(file_handle)


def singleCol(conn, query):
    return [i[0] for i in conn.execute(query).fetchall()]


def multiCol(conn, query):
    return [list(i) for i in conn.execute(query).fetchall()]


'''
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
'''
namespace_dict = {
    'P': 'ps2ps4us:v2', 'G': 'ps2ps4us:v2',
    'S': 'ps2ps4us:v2', 'Cr': 'ps2ps4us:v2',
    'Ce': 'ps2ps4eu:v2', 'L': 'ps2ps4eu:v2',
    'C': 'ps2:v2', 'E': 'ps2:v2', 'M': 'ps2:v2'}
server_id_dict = {
    'G': '1000', 'P': '1001', 'Cr': '1002',
    'S': '1003', 'Ce': '2000', 'L': '2001',
    'C': '1', 'E': '17', 'M': '10'}

server_name_dict = {
    'P': 'Palos', 'G': 'Genudine',
    'Ce': 'Ceres', 'S': 'Searhus',
    'L': 'Lithcorp', 'C': 'Connery',
    'E': 'Emerald', 'M': 'Miller',
    'Cr': 'Crux'}


def fetch_url(url):
    # Fetch the data from url
    retries = 0
    while retries < 5:
        try:
            jsonObj = urllib.request.urlopen(url)
            decoded = json.loads(jsonObj.read().decode('utf8'))
            return decoded
        except Exception as err:
            root.info(str(err))
            root.info('sleeping for 35 seconds before retrying again')
            time.sleep(35)
        retries += 1


eset_schema = '''
    Create table if not exists {}Eset (
        Source TEXT,
        Target TEXT,
        Status TEXT
    )'''

node_schema = '''
    Create table if not exists {}Node (
        Id PRIMARY KEY,
        name TEXT,
        faction TEXT,
        br INTEGER,
        outfitTag TEXT,
        outfitId INTEGER,
        outfitSize INTEGER,
        creation_date INTEGER,
        login_count INTEGER,
        minutes_played INTEGER,
        last_login_date INTEGER,
        kills INTEGER,
        deaths INTEGER
    )'''

history_schema = '''Create table if not exists {}History (
        Id PRIMARY KEY,
        history TEXT
    )'''

stat_history_schema = '''
    INSERT or replace INTO {}History (Id, history) VALUES(?, ?)
    '''

char_data_schema = '''
    INSERT or replace INTO {}Node (
        Id, name, faction, br,
        outfitTag, outfitId, outfitSize,
        creation_date, login_count, minutes_played, last_login_date,
        kills, deaths)
        Values(?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''


class main_data_crawler():
    """
    Contains all the methods settings etc needed to build the friend-list graph
    and fetch node attributes of a server in planetside2. The class takes the
    initial of the server to make a graph of when it is first called.
    """

    def __init__(self, server_inital):

        """
        This limits strain on the database by restricting our attention to only
        those nodes active in last 44.25 days."""
        self.curTime = time.mktime(time.localtime())
        self.DT = datetime.datetime.now()
        self.tSinceLogin = self.curTime - 3824794
        # Using the dictionaries above get the name space server Id and
        # server name of the server we wish to crawl.
        # Remember you can change these class variables if needed.
        self.namespace = namespace_dict[server_inital]
        self.server_id = server_id_dict[server_inital]
        self.server_name = server_name_dict[server_inital]

        self.table_name = self.server_name + self.DT.strftime("%B") +\
            str(self.DT.day) + str(self.DT.year)

        # The set done contains every node we have already examined
        # including those rejected as too old or otherwise invalid.
        self.done = set()

        # idDict stores the data we have collected the key is the Id of the
        # node while the values are the values of its friends.
        self.idDict = {}

        # I have two computers and thus two paths to the Dropbox.
        # You will want to replace self.mypath with whatever path
        # you want to use for storing your data.
        self.mypath = os.path.join('D:', 'Dropbox', 'Dropbox', 'Data2018')

        # The archive database saves the responses from the API so no API call
        # is ever done twice, this is new this version.
        root.info(self.mypath + '\\archive.db')
        self.archive = sqlite3.connect(self.mypath + '\\archive.db')

        # Create the Edge and Node tables for raw data
        self.archive.execute('Create table if not exists ' + self.table_name +
                             'Edge (Id Primary key,raw TEXT)')
        self.archive.execute('Create table if not exists ' + self.table_name +
                             'Node (Id Primary key, raw TEXT)')

        # The seed_node table records the set of seed nodes just in case it is
        # needed for debugging or some unforeseen purpose.
        self.archive.execute('Create table if not exists \
                              seed_nodes (name TEXT, seed_nodes TEXT)')

        # This database stores the unpacked data in the format used later.
        self.database = sqlite3.connect(self.mypath + '\\' + self.server_name + '.db')

        # the data in two tables Eset and Node which have the format my code
        # actually uses and a third table history stores stat history data in
        # case I want to do something with that later.
        self.database.execute(eset_schema.format(self.table_name))
        self.database.execute(node_schema.format(self.table_name))
        self.database.execute(history_schema.format(self.table_name))

        # Get the starting nodes from the leader-boards.
        # If we already have seed nodes for the day simply retrieve them,
        # otherwise gather some.
        existing_seeds = singleCol(self.archive, 'SELECT name from seed_nodes')

        if self.table_name in existing_seeds:
            seed = singleCol(
                self.archive,
                'SELECT seed_nodes from seed_nodes where name = "' +
                self.table_name + '"')[0]
            self.listToCheck = seed.split(',')
        else:
            self.listToCheck = self.leader_board_sample(75)

    # Gathers edges then gathers information on the nodes.
    def run(self):
        root.info('Gathering edges')
        self.get_friendlist_network()
        root.info('Gathering node attributes')
        self.get_node_attributes()

    # Crawls the friend list network.
    def get_friendlist_network(self):
        # Get raw responses from the server for the nodes we got from the
        # leader board earlier.
        self.get_friends(self.listToCheck)
        while len(self.listToCheck) > 0:
            self.expand_graph()
            root.info('Nodes left to check %s' % str(len(self.listToCheck)))

    def catagorize_friends(self, friend):
        '''
        Edges status logic as follows:
        Old: Friends who have not logged on since self.tSinceLogin
        cross server: Friends who are on different servers
        Both: both old and cross server (these are so rare...)
        normal: The rest
        '''
        Id = friend.get('character_id', -1)
        world_id = friend.get('world_id', -1)
        last_online = int(friend.get('last_login_time', -1))

        self.done.add(Id)
        if last_online > self.tSinceLogin:
            if int(world_id) == int(self.server_id):
                return 'normal'
            else:
                return 'cross server'
        else:
            if int(world_id) == int(self.server_id):
                return 'old'
            else:
                return 'both'
        return 'error'

    # Updates the list of nodes to Check and manages what nodes to ask the API
    # about, also saves data to the SQL database.
    def expand_graph(self):
        Queue = set()
        Check = set()
        # Get the list of nodes we have server responses for.
        valid = list(self.idDict.keys())
        for i in self.listToCheck:
            # If i is in the id_dict we unpack its friends-list and go through
            # each of its friends to determine if we traverse them as well.
            # Other wise we add it to the Queue.
            if i in valid:
                friend_list = self.idDict[i]
                self.done.add(i)
                for friend in friend_list:

                    Id = friend.get('character_id', -1)

                    if Id not in self.done:

                        status = self.catagorize_friends(friend)
                        # Add normal characters to the Queue.
                        if status == 'normal':
                            Queue.add(Id)

                        # Inserts the information into the Eset.
                        self.database.execute(
                            'INSERT OR REPLACE INTO '
                            + self.table_name +
                            'Eset (Source,Target,Status) Values(?,?,?)',
                            (i, Id, status))
            else:
                Check.add(i)
                Queue.add(i)
        Check = list(Check)
        Queue = list(Queue)
        root.info('Queue len %s' % len(Queue))
        root.info('Query len %s' % len(Check))
        # If there are nodes in check, fetch repeat.
        if len(Check) > 0:
            self.idDict = self.get_friends(Check)
        # Then add those nodes to the list of nodes to check.
        self.listToCheck = Queue
        self.database.commit()

    def get_friends(self, to_check):
        '''
        Returns a dictionary where the keys are Ids and values are
        their friends lists
        '''
        root.info('Gathering friendlists')
        start_time = time.mktime(time.localtime())
        # Load existing values
        idDict = {}
        # All nodes we have a archived friends-list for already.
        archive_id = singleCol(
            self.archive,
            'Select Id from ' + self.table_name + 'Edge')
        # List of all nodes for which no archived value exists.
        remaining_nodes = [n for n in to_check if n not in archive_id]
        for l in self.chunks(remaining_nodes, 40):

            L = list(set(l))
            url = ('http://census.daybreakgames.com/s:GraphSearch/get/'
                   + self.namespace + '/characters_friend/?character_id='
                   + ','.join(L)
                   + '&c:resolve=world&c:show=character_id,world_id')
            time.sleep(2.0)
            root.info(url)
            decoded = fetch_url(url)
            results = decoded['characters_friend_list']
            for x in results:
                # First dump the raw results of the call into a table
                try:
                    self.archive.execute(
                        'INSERT OR REPLACE into ' + self.table_name
                        + 'Edge (Id,raw) VALUES(?,?)',
                        (x['character_id'], json.dumps(x))
                    )
                except Exception:
                    # Usually when/if this fails its because the server is down
                    root.info('archive failure')
                    root.info('%s %s' % (x['character_id'], json.dumps(x)))
                    if 'error' in str(decoded):
                        root.info('Server down')
                        exit
                    else:
                        raise
            for f in results:
                idDict[f['character_id']] = f['friend_list']
            self.archive.commit()
        # Load in the friends-list from any stored results we may already have
        archived_friends_lists = self.sql_columns_to_dicts(
            'Edge', 'raw', self.archive)
        for l in [i for i in to_check if i not in remaining_nodes]:
            f = json.loads(archived_friends_lists[l])
            try:
                idDict[f['character_id']] = f['friend_list']
            except Exception:
                root.info('get friends error')
                root.info(l)
                root.info(f)
                raise
        root.info('Elapsed time: %s' % str(
            (time.mktime(time.localtime())-start_time)))
        return idDict

    # Gathers all node attributes.
    def get_node_attributes(self):
        edges = multiCol(
            self.database,
            'SELECT Source,Target from ' + self.table_name
            + 'Eset where Status="normal"')
        G = nx.Graph()
        G.add_edges_from(edges)
        self.getCharData(G.nodes())
        self.interp_character_data()

    def getCharData(self, nodes):
        # Gets character attributes for each found in the friend lists
        archive_id = singleCol(
            self.archive, 'Select Id from ' + self.table_name + 'Node')
        remaining_nodes = [n for n in nodes if n not in archive_id]
        re_count = len(remaining_nodes)
        root.info('Number of nodes in graph is: {} \
            Number of unarchived nodes is: {}'.format(len(nodes), re_count)
        )
        # Break the list up into chunks of 40
        smallLists = self.chunks(remaining_nodes, 40)
        i = 0
        for l in smallLists:
            # After 5000 iterations print the progress %.
            if i % 5000 == 0:
                root.info(
                    'looking up data completion is at %s ' % i)
            url = ('http://census.daybreakgames.com/s:GraphSearch/get/'
                   + self.namespace + '/character/?character_id='
                   + ','.join(l)
                   + '&c:resolve=outfit,name,stats,times,stat_history')
            decoded = fetch_url(url)
            results = decoded['character_list']
            for x in results:
                # Unpack the server response and add each to the archive.
                try:
                    self.archive.execute(
                        'INSERT OR REPLACE into '
                        + self.table_name
                        + 'Node (Id,raw) VALUES(?,?)',
                        (x['character_id'], json.dumps(x)))
                except Exception:
                    root.info('archive failure')
                    if 'error' in str(decoded):
                        root.info('Server down')
                        exit
                    else:
                        raise
            self.archive.commit()
            i = i + 40
            # 2 second wait seems to be enough to avoid hitting API soft limit
            time.sleep(2.0)

    def interp_character_data(self):
        '''Unpacks the character data gathered previously,
        reading the raw data from the archive and writing it into the database.
        '''
        completed_id = singleCol(
            self.database, 'SELECT Id from ' + self.table_name + 'Node')
        results = []
        get_unpacked = 'SELECT Id,raw from {}Node'.format(self.table_name)
        for raw in multiCol(self.archive, get_unpacked):
            if raw[0] not in completed_id:
                results.append(json.loads(raw[1]))
        # Unpack and add it to the snapshots.
        for char_info in results:
            # Basic avatar information.
            Id = int(char_info.get('character_id', '0000000000000000000'))
            name = char_info['name'].get('first', 'not available')
            faction_id = char_info.get('faction_id', -1)
            faction = {'1': 'VS', '2': 'NC', '3': 'TR'}.get(
                faction_id, 'has no faction')
            br = char_info.get('battle_rank', {'value': '-1'})['value']
            # Time data:
            tInfo = char_info.get('times')
            creation_date = tInfo.get('creation', '0')
            login_count = tInfo.get('login_count', '0')
            minutes_played = tInfo.get('minutes_played', '0')
            last_login_date = tInfo.get('last_login', '0')
            # Outfit data:
            o = char_info.get('outfit', {'placeholder': 'error2'})
            outfitTag = o.get('alias', -1)
            # outfitName = o.get('name', 'not available')
            outfitId = o.get('outfit_id', -1)
            outfitSize = o.get('member_count', -1)
            # Stat history is formatted differently, it returns a list
            # of stats of dictionaries containing the stat history:
            stats = char_info.get(
                'stats', {'placeholder': 'error2'}).get('stat_history')
            if type(stats) == list:
                death_stats = [sd for sd in stats if sd['stat_name'] == 'deaths']
                kill_stats = [sd for sd in stats if sd['stat_name'] == 'kills']

                if len(death_stats) != 1:
                    root.error(f"Incorrect number of kill stats {death_stats}")
                if len(kill_stats) != 1:
                    root.error(f"Incorrect number of kill stats {kill_stats}")
                kills = death_stats[0].get('all_time', -1)
                deaths = kill_stats[0].get('all_time', -1)
            else:
                kills, deaths = -1, -1
            self.database.execute(stat_history_schema.format(self.table_name),
                                  (Id, json.dumps(stats)))

            self.database.execute(
                char_data_schema.format(self.table_name),
                (Id, name, faction, br, outfitTag, outfitId, outfitSize,
                 creation_date, login_count, minutes_played,
                 last_login_date, kills, deaths))

        self.database.commit()

    def leader_board_sample(self, limit=50):
        '''
        I have used more than one method to get the initial list of
        character Ids. The first version simply used the id's of characters
        I was knew of. This new version is a bit less biased It gathers the
        players who were in the top limit places on the current leader-board
        for all areas of the leader-board available.
        Note that all leader-board stats are strongly correlated.
        '''

        seed_ids = []
        for leaderboard_type in ['Kills', 'Time', 'Deaths', 'Score']:
            url_chunks = [
                'http://census.daybreakgames.com/s:GraphSearch/get/',
                self.namespace,
                '/leaderboard/?name=',
                leaderboard_type,
                '&period=Forever&world=',
                self.server_name,
                '&c:limit=',
                str(limit)]
            url = ''.join(url_chunks)
            root.info(url)
            decoded = fetch_url(url)
            try:
                H = decoded['leaderboard_list']
            except Exception:
                root.info(decoded)
                root.info(url)
            for characters in H:
                Id = characters.get('character_id')
                if Id is not None:
                    seed_ids.append(Id)
        unique = list(set(seed_ids))
        # Record the starting nodes for debugging. The busy_timeout prevents
        # a issue where sqlite3 was not waiting long enough.
        # It probably isn't needed but....
        self.archive.execute("PRAGMA busy_timeout = 30000")
        self.archive.execute(
            'INSERT INTO seed_nodes (name,seed_nodes) VALUES(?,?)',
            (self.table_name, ','.join(unique)))
        return seed_ids

    # Returns the graph and writes it to the desktop for testing in Gephi.
    def save_graph_to_graphml(self, xtend=''):
        graphml_attrs = [
            'name', 'faction', 'br', 'outfitTag', 'outfitId',
            'outfitSize', 'creation_date', 'login_count', 'minutes_played',
            'last_login_date']
        edge_raw = multiCol(
            self.database,
            'SELECT * FROM ' + self.table_name + 'Eset where Status="normal"')
        G = nx.Graph()
        for edge in edge_raw:
            if edge[2] == 'normal':
                G.add_edge(edge[0], edge[1])
                G[edge[0]][edge[1]]['status'] = edge[2]
        archive_id = singleCol(
            self.archive,
            'Select Id from ' + self.table_name + 'Node')
        remaining_nodes = [n for n in G.nodes() if n not in archive_id]
        root.info(remaining_nodes)
        root.info('deleted for being problems')
        G.remove_nodes_from(remaining_nodes)
        for attr in graphml_attrs:
            try:
                G = self.my_set_thing(G, attr, self.sql_columns_to_dicts(
                    'Node', attr, self.database))
            except Exception:
                root.info('failure on %s' % attr)
                raise
        nx.write_graphml(G, 'C:\\' + self.table_name + 'test.graphml')

        return G

    def my_set_thing(self, G, attri_name, a_dict):
        '''
        Sets all nodes in graph G with a new attribute named attri_name using
        values found in a_dict. Networkx has a function that does this but it
        always throws errors if the dict is missing any values in the graph.'''

        for n in G.nodes():
            G.node[n][attri_name] = a_dict[n]
        return G

    def sql_columns_to_dicts(self, table, col_name, connection):
        '''
        Converts a SQL column to a dictionary keys are the character Id and
        values are whatever is in the column named col_name'''
        d = {}
        val = multiCol(
            connection,
            'SELECT Id,' + col_name + ' FROM ' + self.table_name + table)
        for i in val:
            d[str(i[0])] = i[1]
        return d

    def clear_results(self):
        '''
        Erase the tables in the database created by this class.
        In theory there is no situation where we would need to remove archived
        values.
        '''
        self.database.execute('DROP TABLE '+self.table_name+'Eset')
        self.database.execute('DROP TABLE '+self.table_name+'Node')
        self.database.execute('DROP TABLE '+self.table_name+'History')

    def chunks(self, alist, n):
        '''Breaks a list into a list of length n list.'''
        outList = []
        for i in range(0, len(alist), n):
            outList.append(alist[i:i+n])
        return outList


def run_PS4():
    # Crawl the playstation 4 servers.
    # Note that most of these servers have since been merged together.
    for initials in ['G', 'Cr', 'L', 'S', 'Ce', 'P']:
        x = main_data_crawler(initials)
        root.info('Now crawling %s' % x.server_name)
        x.run()


def run_PC():
    # Crawl the 3 PC servers.
    for initials in ['E', 'C', 'M']:
        x = main_data_crawler(initials)
        root.info('Now crawling %s' % x.server_name)
        x.run()


if __name__ == "__main__":
    run_PC()
