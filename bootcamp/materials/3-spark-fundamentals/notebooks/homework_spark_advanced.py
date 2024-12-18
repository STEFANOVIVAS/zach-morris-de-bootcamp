{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "0ddedef7-f290-4d40-90e3-947fd0518ae3",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Intitializing Scala interpreter ..."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": [
       "Spark Web UI available at http://d86d86c5781e:4041\n",
       "SparkContext available as 'sc' (version = 3.5.1, master = local[*], app id = local-1734393821728)\n",
       "SparkSession available as 'spark'\n"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": [
       "import org.apache.spark.sql.SparkSession\n",
       "import org.apache.spark.sql.functions.col\n",
       "import org.apache.spark.storage.StorageLevel\n",
       "spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@f81ff08\n"
      ]
     },
     "execution_count": 1,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import org.apache.spark.sql.SparkSession\n",
    "import org.apache.spark.sql.functions.{col}\n",
    "import org.apache.spark.storage.StorageLevel\n",
    "\n",
    "val spark = SparkSession.builder()\n",
    "  .appName(\"IcebergTableManagement\") \n",
    "  .config(\"spark.executor.memory\", \"4g\")\n",
    "  .config(\"spark.driver.memory\", \"4g\")\n",
    "  .config(\"spark.sql.shuffle.partitions\", \"200\") // Fine for large datasets\n",
    "  .config(\"spark.sql.files.maxPartitionBytes\", \"134217728\") // Optional: 128 MB is default\n",
    "  .config(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\") // Optional: Disable broadcast join\n",
    "  .config(\"spark.dynamicAllocation.enabled\", \"true\") // Helps with resource allocation\n",
    "  .config(\"spark.dynamicAllocation.minExecutors\", \"1\") // Ensure minimum resources\n",
    "  .config(\"spark.dynamicAllocation.maxExecutors\", \"50\") // Scalable resource allocation\n",
    "  .getOrCreate()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "0042f27d-5489-4441-a14a-6f03a53e5273",
   "metadata": {},
   "source": [
    "### 1 question"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "bd4b1cf6-246d-4d4e-8b89-a1d471de8a8c",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "medals: org.apache.spark.sql.DataFrame = [medal_id: bigint, sprite_uri: string ... 10 more fields]\n",
       "medalsMatchesPlayers: org.apache.spark.sql.DataFrame = [match_id: string, player_gamertag: string ... 2 more fields]\n",
       "explicitBroadcastMedalsMatchesPlayers: org.apache.spark.sql.DataFrame = [match_id: string, player_gamertag: string ... 14 more fields]\n"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val medals = spark.read.option(\"header\", \"true\")\n",
    "                        .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/medals.csv\")\n",
    "\n",
    "val medalsMatchesPlayers = spark.read.option(\"header\", \"true\")\n",
    "                        .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/medals_matches_players.csv\")\n",
    "\n",
    "val explicitBroadcastMedalsMatchesPlayers = medalsMatchesPlayers.as(\"mmp\").join(broadcast(medals).as(\"m\"), $\"mmp.medal_id\" === $\"m.medal_id\")  \n",
    "\n",
    "\n",
    "                        "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "637dd591-9d84-4bc9-863b-51c06bc9070b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "maps: org.apache.spark.sql.DataFrame = [mapid: string, name: string ... 1 more field]\n",
       "matches: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 8 more fields]\n",
       "explicitBroadcastMatchesMaps: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 11 more fields]\n"
      ]
     },
     "execution_count": 3,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val maps = spark.read.option(\"header\", \"true\")\n",
    "                       .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/maps.csv\")\n",
    "\n",
    "val matches = spark.read.option(\"header\", \"true\")\n",
    "                        .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/matches.csv\")\n",
    "\n",
    "val explicitBroadcastMatchesMaps = matches.as(\"m\").join(broadcast(maps).as(\"maps\"), $\"m.mapid\" === $\"maps.mapid\")  \n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "d168263e-ee53-4ae6-8441-e183e4140800",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "explicitBroadcast: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 11 more fields]\n"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    " val explicitBroadcast = matches.as(\"m\").join(broadcast(maps).as(\"maps\"), $\"m.mapid\" === $\"maps.mapid\")  "
   ]
  },
  {
   "cell_type": "markdown",
   "id": "98a701d8-f270-4131-8bfc-a14225652806",
   "metadata": {},
   "source": [
    "### 2 question"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "f38a98d9-461f-48e1-bbd5-4cfd2123256b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+------------+--------------------+-------------------+\n",
      "|            match_id|               mapid|is_team_game|         playlist_id|    completion_date|\n",
      "+--------------------+--------------------+------------+--------------------+-------------------+\n",
      "|0df7e36f-9501-483...|ca737f8f-f206-11e...|        true|2323b76a-db98-4e0...|2016-08-07 00:00:00|\n",
      "|2ffc4169-2dbb-47a...|cb914b9e-f206-11e...|        true|c98949ae-60a8-43d...|2016-06-03 00:00:00|\n",
      "|d3a160d6-c93b-497...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|2015-11-29 00:00:00|\n",
      "|3e35057a-130e-424...|c74c9d0f-f206-11e...|        true|0bcf2be1-3168-4e4...|2015-11-29 00:00:00|\n",
      "|5a1a43d3-04ac-484...|cb914b9e-f206-11e...|        true|f27a65eb-2d11-496...|2015-11-29 00:00:00|\n",
      "|daecefdc-ef8e-41e...|c74c9d0f-f206-11e...|        true|0bcf2be1-3168-4e4...|2015-11-29 00:00:00|\n",
      "|64d9cd93-7e53-46b...|cdee4e70-f206-11e...|        true|f27a65eb-2d11-496...|2015-11-29 00:00:00|\n",
      "|c20c3350-1a17-481...|ce1dc2de-f206-11e...|        true|f27a65eb-2d11-496...|2015-11-29 00:00:00|\n",
      "|10170da3-e252-401...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|2015-11-29 00:00:00|\n",
      "|34b24600-a7cb-40e...|c7edbf0f-f206-11e...|        true|f72e0ef0-7c4a-430...|2015-11-29 00:00:00|\n",
      "|a4701021-7d2a-457...|cebd854f-f206-11e...|       false|d0766624-dbd7-453...|2016-02-05 00:00:00|\n",
      "|a9bbbc99-b03a-4ce...|c74c9d0f-f206-11e...|        true|780cc101-005c-4fc...|2016-02-05 00:00:00|\n",
      "|5f8b7a8d-3103-489...|cbcea2c0-f206-11e...|        true|2323b76a-db98-4e0...|2016-02-05 00:00:00|\n",
      "|b98285cf-0296-41a...|cc040aa1-f206-11e...|        true|2323b76a-db98-4e0...|2016-02-05 00:00:00|\n",
      "|9bc8a5a4-8d00-4f3...|cbcea2c0-f206-11e...|        true|892189e9-d712-4bd...|2016-02-05 00:00:00|\n",
      "|14b60c7a-afb1-403...|c7edbf0f-f206-11e...|        NULL|f72e0ef0-7c4a-430...|2015-12-24 00:00:00|\n",
      "|552b4a88-73b4-415...|cae999f0-f206-11e...|        true|0e39ead4-383b-445...|2015-12-24 00:00:00|\n",
      "|acd50816-c02d-421...|cae999f0-f206-11e...|        true|0e39ead4-383b-445...|2015-12-24 00:00:00|\n",
      "|0eedeb83-f9da-46f...|c7edbf0f-f206-11e...|        NULL|f72e0ef0-7c4a-430...|2015-12-24 00:00:00|\n",
      "|27d403ba-6a5b-468...|c7edbf0f-f206-11e...|        NULL|f72e0ef0-7c4a-430...|2015-12-24 00:00:00|\n",
      "+--------------------+--------------------+------------+--------------------+-------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "matchesBucketedselect: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 8 more fields]\n",
       "distinctDates: Array[org.apache.spark.sql.Row] = Array([2016-03-13 00:00:00.0], [2016-03-11 00:00:00.0], [2016-03-10 00:00:00.0], [2016-01-30 00:00:00.0], [2016-03-27 00:00:00.0], [2016-04-10 00:00:00.0], [2016-01-18 00:00:00.0], [2016-02-01 00:00:00.0], [2015-12-14 00:00:00.0], [2016-02-03 00:00:00.0], [2016-04-30 00:00:00.0], [2016-03-05 00:00:00.0], [2016-04-15 00:00:00.0], [2016-05-21 00:00:00.0], [2015-10-31 00:00:00.0], [2016-01-22 00:00:00.0], [2016-02-09 00:00:00.0], [2016-03-17 00:00:00.0], [2016-04-04 00:00:00.0], [2016-05-08 00:00:00.0], [2016-01-21 00:00:00.0], [2015-10-28 00:00:00.0], [2016-03-30 00:00:00.0], [2016-05-03 00:00:00.0], [2016-02-04 00:00:00.0], [2015-11-...\n"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val matchesBucketedselect = spark.read.option(\"header\", \"true\")\n",
    "  .option(\"inferSchema\", \"true\")\n",
    "  .csv(\"/home/iceberg/data/matches.csv\")\n",
    "\n",
    "// Get distinct completion dates\n",
    "val distinctDates = matchesBucketedselect.select(\"completion_date\").distinct().collect()\n",
    "\n",
    "spark.sql(\"\"\"DROP TABLE IF EXISTS bootcamp.matches_bucketed\"\"\")\n",
    "// Create the Iceberg table if it doesn't exist\n",
    "val bucketedDDL = \"\"\"\n",
    "CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (\n",
    "    match_id STRING,\n",
    "    mapid STRING,\n",
    "    is_team_game BOOLEAN,\n",
    "    playlist_id STRING,\n",
    "    completion_date TIMESTAMP\n",
    ")\n",
    "USING iceberg\n",
    "PARTITIONED BY (completion_date, bucket(16, match_id))\n",
    "\"\"\"\n",
    "spark.sql(bucketedDDL)\n",
    "\n",
    "// Process data in chunks based on completion_date\n",
    "distinctDates.foreach { row =>\n",
    "  val date = row.getAs[java.sql.Timestamp](\"completion_date\")\n",
    "  val filteredMatches = matchesBucketedselect.filter(col(\"completion_date\") === date)\n",
    "  \n",
    "  // Repartition and persist the filtered data\n",
    "  val optimizedMatches = filteredMatches\n",
    "    .select($\"match_id\",$\"mapid\", $\"is_team_game\", $\"playlist_id\", $\"completion_date\")\n",
    "    .repartition(16, $\"match_id\")\n",
    "    .persist(StorageLevel.MEMORY_AND_DISK)\n",
    "    \n",
    "  optimizedMatches.write\n",
    "    .mode(\"append\")\n",
    "    .bucketBy(16, \"match_id\")\n",
    "    .partitionBy(\"completion_date\")\n",
    "    .saveAsTable(\"bootcamp.matches_bucketed\")\n",
    "}\n",
    "\n",
    "// Verify the data in the table\n",
    "val result = spark.sql(\"SELECT * FROM bootcamp.matches_bucketed\")\n",
    "result.show()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "2d0f4f1a-7c28-460a-a04f-c449f4888831",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "matchDetails: org.apache.spark.sql.DataFrame = [match_id: string, player_gamertag: string ... 34 more fields]\n",
       "bucketedDetailsDDL: String =\n",
       "\"\n",
       " CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (\n",
       "     match_id STRING,\n",
       "     player_gamertag STRING,\n",
       "     player_total_kills INTEGER,\n",
       "     player_total_deaths INTEGER\n",
       " )\n",
       " USING iceberg\n",
       " PARTITIONED BY (bucket(16, match_id));\n",
       " \"\n"
      ]
     },
     "execution_count": 6,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "\n",
    "val matchDetails =  spark.read.option(\"header\", \"true\")\n",
    "                        .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/match_details.csv\")\n",
    "\n",
    "spark.sql(\"\"\"DROP TABLE IF EXISTS bootcamp.match_details_bucketed\"\"\")\n",
    "val bucketedDetailsDDL = \"\"\"\n",
    " CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (\n",
    "     match_id STRING,\n",
    "     player_gamertag STRING,\n",
    "     player_total_kills INTEGER,\n",
    "     player_total_deaths INTEGER\n",
    " )\n",
    " USING iceberg\n",
    " PARTITIONED BY (bucket(16, match_id));\n",
    " \"\"\"\n",
    "spark.sql(bucketedDetailsDDL)\n",
    "\n",
    "matchDetails.select(\n",
    "     $\"match_id\", $\"player_gamertag\", $\"player_total_kills\", $\"player_total_deaths\")\n",
    "     .write.mode(\"overwrite\")\n",
    "   .bucketBy(16, \"match_id\").saveAsTable(\"bootcamp.match_details_bucketed\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "19ed9c89-0c6a-4b5f-83bd-f7c40e38ba99",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "medalsMatchesPlayers: org.apache.spark.sql.DataFrame = [match_id: string, player_gamertag: string ... 2 more fields]\n",
       "bucketedMedalsDDL: String =\n",
       "\"\n",
       " CREATE TABLE IF NOT EXISTS bootcamp.medals_match_players_bucketed (\n",
       "     match_id STRING,\n",
       "     player_gamertag STRING,\n",
       "     medal_id STRING,\n",
       "     count INTEGER\n",
       " )\n",
       " USING iceberg\n",
       " PARTITIONED BY (bucket(16, match_id));\n",
       " \"\n"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val medalsMatchesPlayers  =  spark.read.option(\"header\", \"true\")\n",
    "                        .option(\"inferSchema\", \"true\")\n",
    "                        .csv(\"/home/iceberg/data/medals_matches_players.csv\")\n",
    "\n",
    "spark.sql(\"\"\"DROP TABLE IF EXISTS bootcamp.medals_match_players_bucketed\"\"\")\n",
    "val bucketedMedalsDDL = \"\"\"\n",
    " CREATE TABLE IF NOT EXISTS bootcamp.medals_match_players_bucketed (\n",
    "     match_id STRING,\n",
    "     player_gamertag STRING,\n",
    "     medal_id STRING,\n",
    "     count INTEGER\n",
    " )\n",
    " USING iceberg\n",
    " PARTITIONED BY (bucket(16, match_id));\n",
    " \"\"\"\n",
    "spark.sql(bucketedMedalsDDL)\n",
    "\n",
    "medalsMatchesPlayers.select(\n",
    "     $\"match_id\", $\"player_gamertag\", $\"medal_id\", $\"count\")\n",
    "     .write.mode(\"overwrite\")\n",
    "   .bucketBy(16, \"match_id\").saveAsTable(\"bootcamp.medals_match_players_bucketed\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "9e70ca73-3915-4db7-9e4e-59a5c61d8d64",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "joinMatchesPlayersBucketed: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 26 more fields]\n"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val joinMatchesPlayersBucketed=spark.table(\"bootcamp.matches_bucketed\").as(\"mb\")\n",
    "                                .join(spark.table(\"bootcamp.match_details_bucketed\").as(\"mdb\"),$\"mb.match_id\" === $\"mdb.match_id\")\n",
    "                                .join(spark.table(\"bootcamp.medals_match_players_bucketed\").as(\"mmp\"),$\"mb.match_id\" === $\"mmp.match_id\")\n",
    "                                .join(broadcast(medals).as(\"medals\"),$\"mmp.medal_id\" === $\"medals.medal_id\")\n",
    "                                .join(broadcast(maps).as(\"maps\"),$\"mb.mapid\" === $\"maps.mapid\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "2e04db33-2b83-484f-9461-514fe2917de8",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "joinBucketed: org.apache.spark.sql.DataFrame = [match_id: string, mapid: string ... 8 more fields]\n"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val joinBucketed=spark.table(\"bootcamp.matches_bucketed\").as(\"mb\")\n",
    "                                .join(spark.table(\"bootcamp.match_details_bucketed\").as(\"mdb\"),$\"mb.match_id\" === $\"mdb.match_id\")\n",
    "                                .join(spark.table(\"bootcamp.medals_match_players_bucketed\").as(\"mmp\"),$\"mb.match_id\" === $\"mmp.match_id\")\n",
    "                                .select($\"mb.match_id\", $\"mb.mapid\",$\"is_team_game\",$\"playlist_id\",$\"completion_date\",$\"mdb.player_gamertag\",$\"player_total_kills\"\n",
    "                                ,$\"player_total_deaths\",$\"medal_id\",$\"count\")\n",
    "                               "
   ]
  },
  {
   "cell_type": "markdown",
   "id": "cc0f8221-599a-4807-9407-e0e2630cfc97",
   "metadata": {},
   "source": [
    "## Which player averages the most kills per game?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "d499f423-a505-4c50-9051-2db5c70979fe",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------------+--------------------+\n",
      "|player_gamertag|avg_total_kills_game|\n",
      "+---------------+--------------------+\n",
      "|   gimpinator14|               109.0|\n",
      "+---------------+--------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\" SELECT mdb.player_gamertag,avg(player_total_kills) as avg_total_kills_game FROM  bootcamp.matches_bucketed mb \n",
    "            INNER JOIN bootcamp.match_details_bucketed mdb ON\n",
    "            mb.match_id=mdb.match_id\n",
    "            INNER JOIN bootcamp.medals_match_players_bucketed mmpb\n",
    "            ON mb.match_id=mmpb.match_id\n",
    "            GROUP BY mdb.match_id,mdb.player_gamertag\n",
    "            order by avg_total_kills_game desc limit 1\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "b59a831a-cf6e-4b8f-90f8-199732f2e618",
   "metadata": {},
   "source": [
    "## Which playlist gets played the most?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "402ebee3-cb8b-4a6e-a62b-897630ba3a2d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+-----------------+\n",
      "|         playlist_id|count_playlist_id|\n",
      "+--------------------+-----------------+\n",
      "|f72e0ef0-7c4a-430...|             7640|\n",
      "+--------------------+-----------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\" SELECT mb.playlist_id,COUNT(distinct(mb.match_id)) as count_playlist_id FROM  bootcamp.matches_bucketed mb \n",
    "            INNER JOIN bootcamp.match_details_bucketed mdb ON\n",
    "            mb.match_id=mdb.match_id\n",
    "            INNER JOIN bootcamp.medals_match_players_bucketed mmpb\n",
    "            ON mb.match_id=mmpb.match_id\n",
    "            GROUP BY mb.playlist_id\n",
    "            order by count_playlist_id desc limit 1\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "b77c8b10-99ce-4308-8d7b-845019dbcc06",
   "metadata": {},
   "source": [
    "## Which map gets played the most?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "4c074b00-702e-49d7-ad7c-29a3a1859c79",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------------+\n",
      "|               mapid|count_map_id|\n",
      "+--------------------+------------+\n",
      "|c7edbf0f-f206-11e...|        7032|\n",
      "+--------------------+------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\" SELECT mb.mapid,COUNT(distinct(mb.match_id)) as count_map_id FROM  bootcamp.matches_bucketed mb \n",
    "            INNER JOIN bootcamp.match_details_bucketed mdb ON\n",
    "            mb.match_id=mdb.match_id\n",
    "            INNER JOIN bootcamp.medals_match_players_bucketed mmpb\n",
    "            ON mb.match_id=mmpb.match_id\n",
    "            GROUP BY mb.mapid\n",
    "            order by count_map_id desc limit 1\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "9c454898-de9e-4bf9-b116-401c4d563fb7",
   "metadata": {},
   "source": [
    "## Which map do players get the most Killing Spree medals on?"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "4682f790-8236-43a9-8133-7fbec2ea3665",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------------+\n",
      "|               mapid|count_map_id|\n",
      "+--------------------+------------+\n",
      "|c7edbf0f-f206-11e...|        6744|\n",
      "+--------------------+------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\" SELECT mb.mapid,sum(mmpb.count) as count_map_id FROM  bootcamp.matches_bucketed mb \n",
    "            INNER JOIN bootcamp.medals_match_players_bucketed mmpb\n",
    "            ON mb.match_id=mmpb.match_id\n",
    "            where mmpb.medal_id='2430242797'\n",
    "            GROUP BY mb.mapid\n",
    "            order by count_map_id desc limit 1\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "0a34f571-f916-41ae-bd4c-74dc2cbc31bb",
   "metadata": {},
   "source": [
    "## Part 3"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "7bb03ab4-22f1-4cdc-9f59-912b16bce7fb",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "res7: org.apache.spark.sql.DataFrame = []\n"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\" DROP TABLE IF EXISTS bootcamp.statistics_unsorted \"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "945ecd41-951d-4b49-b04f-9efa63c24dff",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- match_id: string (nullable = true)\n",
      " |-- mapid: string (nullable = true)\n",
      " |-- is_team_game: boolean (nullable = true)\n",
      " |-- playlist_id: string (nullable = true)\n",
      " |-- completion_date: timestamp (nullable = true)\n",
      " |-- player_gamertag: string (nullable = true)\n",
      " |-- player_total_kills: integer (nullable = true)\n",
      " |-- player_total_deaths: integer (nullable = true)\n",
      " |-- medal_id: long (nullable = true)\n",
      " |-- count: integer (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "joinBucketed.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "6eeafbb4-5cd8-4b78-afc5-43f29b46302c",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "res9: org.apache.spark.sql.DataFrame = []\n"
      ]
     },
     "execution_count": 16,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS bootcamp.statistics_unsorted (\n",
    " match_id string ,\n",
    " mapid string ,\n",
    " is_team_game boolean ,\n",
    " playlist_id string ,\n",
    " completion_date timestamp ,\n",
    " player_gamertag string ,\n",
    " player_total_kills integer ,\n",
    " player_total_deaths integer , \n",
    " medal_id long ,\n",
    " count integer \n",
    ")\n",
    "USING iceberg;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "94491f62-a6d9-45de-9038-a35e3254e851",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "res10: org.apache.spark.sql.DataFrame = []\n"
      ]
     },
     "execution_count": 17,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\" DROP TABLE IF EXISTS bootcamp.statistics_sorted_match_id \"\"\")\n",
    "spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS bootcamp.statistics_sorted_match_id (\n",
    " match_id string ,\n",
    " mapid string ,\n",
    " is_team_game boolean ,\n",
    " playlist_id string ,\n",
    " completion_date timestamp ,\n",
    " player_gamertag string, \n",
    " player_total_kills integer ,\n",
    " player_total_deaths integer,  \n",
    " medal_id long ,\n",
    " count integer \n",
    ")\n",
    "USING iceberg;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "8e1417a9-b305-4498-9c09-1aebc91fe921",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "res11: org.apache.spark.sql.DataFrame = []\n"
      ]
     },
     "execution_count": 18,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\" DROP TABLE IF EXISTS bootcamp.statistics_sorted_map_id \"\"\")\n",
    "spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS bootcamp.statistics_sorted_map_id (\n",
    " match_id string ,\n",
    " mapid string ,\n",
    " is_team_game boolean ,\n",
    " playlist_id string ,\n",
    " completion_date timestamp ,\n",
    " player_gamertag string ,\n",
    " player_total_kills integer ,\n",
    " player_total_deaths integer,  \n",
    " medal_id long ,\n",
    " count integer \n",
    ")\n",
    "USING iceberg;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "ca9dc09d-be5e-4316-93db-f31d794d4c82",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "res12: org.apache.spark.sql.DataFrame = []\n"
      ]
     },
     "execution_count": 19,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"\"\" DROP TABLE IF EXISTS bootcamp.statistics_sorted_playlist \"\"\")\n",
    "spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS bootcamp.statistics_sorted_playlist (\n",
    " match_id string ,\n",
    " mapid string ,\n",
    " is_team_game boolean, \n",
    " playlist_id string, \n",
    " completion_date timestamp ,\n",
    " player_gamertag string, \n",
    " player_total_kills integer ,\n",
    " player_total_deaths integer , \n",
    " medal_id long ,\n",
    " count integer \n",
    ")\n",
    "USING iceberg;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "1f52b963-71d6-49fd-995c-7fdf3658c356",
   "metadata": {},
   "outputs": [],
   "source": [
    "joinBucketed.write.mode(\"append\").saveAsTable(\"bootcamp.statistics_unsorted \")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ac0d9aca-e8d0-44c1-830a-c8db62e000d1",
   "metadata": {},
   "source": [
    "## Sort within partitions by match id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "34d961fd-8361-4c86-be39-2a135c35c06b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "joinBucketedSortMatch: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [match_id: string, mapid: string ... 8 more fields]\n"
      ]
     },
     "execution_count": 21,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val joinBucketedSortMatch=joinBucketed.repartition(16,$\"completion_date\").sortWithinPartitions($\"match_id\")\n",
    "joinBucketedSortMatch.write.mode(\"overwrite\").saveAsTable(\"bootcamp.statistics_sorted_match_id \")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c92aad0e-369b-4c7c-8707-ace69cd254b0",
   "metadata": {},
   "source": [
    "## Sort within partitions by map id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "15c60652-ed91-450d-b9be-a3989100c9b2",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "joinBucketedSortMap: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [match_id: string, mapid: string ... 8 more fields]\n"
      ]
     },
     "execution_count": 22,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val joinBucketedSortMap=joinBucketed.repartition(16,$\"completion_date\").sortWithinPartitions($\"mapid\")\n",
    "joinBucketedSortMap.write.mode(\"overwrite\").saveAsTable(\"bootcamp.statistics_sorted_map_id\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "e9239ee2-8245-4de6-ba63-9d6a185fcebd",
   "metadata": {},
   "source": [
    "## Sort within partitions by playlist"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "d9a45bd7-391a-49e3-9570-3a892f493fb9",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "joinBucketedSortPlaylist: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [match_id: string, mapid: string ... 8 more fields]\n"
      ]
     },
     "execution_count": 23,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "val joinBucketedSortPlaylist=joinBucketed.repartition(16,$\"completion_date\").sortWithinPartitions($\"playlist_id\")\n",
    "joinBucketedSortPlaylist.write.mode(\"overwrite\").saveAsTable(\"bootcamp.statistics_sorted_playlist\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "8700a61e-3caf-40e3-8b4f-24f69d4337e5",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------+---------+------------------+\n",
      "|    size|num_files|   match_id sorted|\n",
      "+--------+---------+------------------+\n",
      "| 5999893|       16|   match_id sorted|\n",
      "|10655109|       16|     map_id sorted|\n",
      "|10548297|       16|playlist_id sorted|\n",
      "| 5740587|       12|          unsorted|\n",
      "+--------+---------+------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "\n",
    "SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'match_id sorted' \n",
    "FROM bootcamp.statistics_sorted_match_id.files\n",
    "UNION ALL\n",
    "SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'map_id sorted' \n",
    "FROM bootcamp.statistics_sorted_map_id.files\n",
    "UNION ALL\n",
    "SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'playlist_id sorted' \n",
    "FROM bootcamp.statistics_sorted_playlist.files\n",
    "UNION ALL\n",
    "SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' \n",
    "FROM bootcamp.statistics_unsorted.files\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "9b0d601f-6088-4d0b-8696-c0f5ecac160f",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "spylon-kernel",
   "language": "scala",
   "name": "spylon-kernel"
  },
  "language_info": {
   "codemirror_mode": "text/x-scala",
   "file_extension": ".scala",
   "help_links": [
    {
     "text": "MetaKernel Magics",
     "url": "https://metakernel.readthedocs.io/en/latest/source/README.html"
    }
   ],
   "mimetype": "text/x-scala",
   "name": "scala",
   "pygments_lexer": "scala",
   "version": "0.4.1"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
