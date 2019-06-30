#!/usr/bin/python3

import feedparser
from os.path import expanduser
from sqlalchemy import create_engine, select
from sqlalchemy import ForeignKey, PrimaryKeyConstraint
from sqlalchemy import MetaData, Sequence
from sqlalchemy import Table, Column, Integer, String, Date
import json
import sys
from argparse import ArgumentParser
from datetime import datetime
from time import mktime, sleep
import subprocess
from mutagen.easyid3 import EasyID3
import re

class Database:
  def __init__(self, path):
    self.engine = create_engine(path)
    self.conn = self.engine.connect()
    self.metadata = MetaData()
    self.casts = Table('podcasts', self.metadata,
      Column('id', Integer, Sequence('podcasts_id_seq'), primary_key = True),
      Column('url', String),
      Column('name', String),
      Column('author', String)
    )
    self.tracks = Table('tracks', self.metadata,
      Column('gid', String),
      Column('podcast', Integer, None, ForeignKey("podcasts.id")),
      Column('title', String),
      Column('description', String),
      Column('published', Date),
      Column('track_url', String),
      Column('track_file', String),
      PrimaryKeyConstraint('gid', 'podcast')
    )
    self.metadata.create_all(self.engine)

  def add(self, url):
    ins = self.casts.insert().values(url = url)
    self.conn.execute(ins)

  def delete(self, id):
    d = self.tracks.delete().where(self.tracks.c.podcast == int(id))
    self.conn.execute(d)
    d = self.casts.delete().where(self.casts.c.id == int(id))
    self.conn.execute(d)


  def list(self, id = None):
    list_query = select([self.casts])
    if id is not None:
      list_query = list_query.where(self.casts.c.id == id)
    return [
      Podcast(self, row["id"], row["url"], row["name"], row["author"])
      for row in self.conn.execute(list_query)
    ]

  def refresh(self, id = None):
    for cast in self.list(id):
      cast.refresh()

  def download(self, directory, id = None, refresh_first = False, update_metadata = False):
    for cast in self.list(id):
      if refresh_first:
        cast.refresh()
      cast.download(directory, update_metadata = update_metadata)

  def update_metadata(self, id = None):
    for cast in self.list(id):
      cast.update_metadata()

  def generate_playlists(self, directory, id = None, refresh_first = False, download_first = None, update_metadata = False, path_subst = None):
    if path_subst is not None:
      if type(path_subst) is str:
        path_subst = path_subst.split("^")
    if download_first is not None:
      self.download(download_first, id = id, refresh_first = refresh_first, update_metadata = update_metadata)
    else: 
      if update_metadata:
        self.update_metadata(id = id)
    for cast in self.list(id):
      safe_file_name = re.sub("['\"]", "", cast.name)
      safe_file_name = re.sub("[^ a-zA-Z0-9]+", " ", safe_file_name)
      safe_file_name = safe_file_name.strip()
      # print(safe_file_name)
      cast.dump_to_m3u("{}/{}.m3u".format(directory, safe_file_name), path_subst = path_subst)

class Podcast:
  def __init__(self, db, id, url, name, author):
    self.db = db
    self.id = id
    self.url = url
    self.name = name
    self.author = author

  def describe(self):
    return "{:02}. \"{}\" by {} ({}...)".format(self.id, self.name, self.author, self.url[:30])

  def refresh(self):
    print("Refreshing {}...".format(self.name))
    data = feedparser.parse(self.url)
    self.name   = data["channel"]["title"]
    self.author = data["channel"]["author"]
    set_meta_update = (
      self.db.casts.update()
                   .where(self.db.casts.c.id == self.id)
                   .values(
                     name = self.name, 
                     author = self.author
                   )
    )
    self.db.conn.execute(set_meta_update)
    existing_identifier_query = (
      select([self.db.tracks.c.gid])
        .where(self.db.tracks.c.podcast == self.id)
    )
    existing_gids = set( 
      row.gid for row in self.db.conn.execute(existing_identifier_query) 
    )
    # print(existing_gids)
    for track in data["items"]:
      # print(trac)
      fields = {}
      fields["gid"] = track["id"]
      fields["podcast"] = self.id
      fields["title"] = track["title"]
      fields["description"] = track["description"]
      if "published_parsed" in track:
        fields["published"] = datetime.fromtimestamp(mktime(track["published_parsed"]))
      elif "date_parsed" in track:
        fields["published"] = datetime.fromtimestamp(mktime(track["date_parsed"]))
      for link in track["links"]:
        if "type" in link and link["type"].split("/")[0] == "audio" and "href" in link:
          fields["track_url"] = link["href"]
          break

      # print(fields["published"])
      if fields["gid"] in existing_gids:
        # print("Update {}: {}".format(self.name, fields["title"]))
        gid = fields.pop("gid")
        del fields["podcast"]
        insert_or_update_track_query = (
          self.db.tracks.update()
                        .values(**fields)
                        .where(self.db.tracks.c.podcast == self.id and
                               self.db.tracks.c.gid == gid)
        )
      else:
        print("Insert {}: {}".format(self.name, fields["title"]))
        insert_or_update_track_query = (
          self.db.tracks.insert()
                        .values(**fields)
        )
      self.db.conn.execute(insert_or_update_track_query)

  def download(self, directory, update_metadata = False):
    print("Downloading {}...".format(self.name))
    for track in self.get_tracks():
      if track["track_file"] is None:
        gid = track["gid"].split("/")[-1]
        file_name = "{}/cast{}-{}.mp3".format(directory, self.id, gid)
        print(file_name)
        print("Downloading (in 3 sec) {} -> {}".format(track["track_url"], file_name))
        sleep(3)
        subprocess.call(["curl", "-L", "-o", file_name, track["track_url"]], stdout = sys.stdout)
        update_track_file = (
          self.db.tracks.update()
                        .values(track_file = file_name)
                        .where(self.db.tracks.c.gid == track["gid"] and 
                               self.db.tracks.c.podcast == self.id)
        )
        self.db.conn.execute(update_track_file)
        if update_metadata:
          self.update_metadata(track, file_name)
      # else:
        # print("Already Have {}".format(track["track_file"]))
        # if update_metadata:
        #   self.update_metadata(track)

  def get_tracks(self):
    return self.db.conn.execute(
      select([self.db.tracks])
        .where(self.db.tracks.c.podcast == self.id)
    )

  def update_metadata(self, track = None, file_override = None):
    if track == None:
      for track in self.get_tracks():
        self.update_metadata(track)
    else:
      if file_override is not None:
        track_file = file_override
      else:
        track_file = track["track_file"]
      if track_file is not None:
        print(track_file)
        # ['album', 'bpm', 'compilation', 'composer', 'copyright', 
        #  'encodedby', 'lyricist', 'length', 'media', 'mood', 'title', 
        #  'version', 'artist', 'albumartist', 'conductor', 'arranger', 
        #  'discnumber', 'organization', 'tracknumber', 'author', 
        #  'albumartistsort', 'albumsort', 'composersort', 'artistsort', 
        #  'titlesort', 'isrc', 'discsubtitle', 'language', 'genre', 
        #  'date', 'originaldate', 'performer:*', 'musicbrainz_trackid', 
        #  'website', 'replaygain_*_gain', 'replaygain_*_peak', 
        #  'musicbrainz_artistid', 'musicbrainz_albumid', 
        #  'musicbrainz_albumartistid', 'musicbrainz_trmid', 'musicip_puid', 
        #  'musicip_fingerprint', 'musicbrainz_albumstatus', 
        #  'musicbrainz_albumtype', 'releasecountry', 'musicbrainz_discid', 
        #  'asin', 'performer', 'barcode', 'catalognumber', 
        #  'musicbrainz_releasetrackid', 'musicbrainz_releasegroupid', 
        #  'musicbrainz_workid', 'acoustid_fingerprint', 'acoustid_id']
        track_meta = EasyID3(track_file)
        track_meta["album"] = self.name
        track_meta["artist"] = self.author
        track_meta["genre"] = "Podcast"
        track_meta["title"] = track["title"]
        track_meta["date"] = "{}-{}-{}".format(track["published"].year, track["published"].month, track["published"].day)
        print(track_meta)
        track_meta.save()

  def dump_to_m3u(self, file, limit = None, extm3u = True, path_subst = None):
    with open(file, "w+") as output:
      tracks = self.get_tracks()
      if limit is not None:
        tracks = tracks[:limit]
      if extm3u:
        output.write("#EXTM3U\n\n")
      tracks = sorted(tracks, key = lambda x: x["published"], reverse = True)
      for track in tracks:
        track_file = track["track_file"]
        if track_file is not None:
          if extm3u:
            output.write("#EXTINF:-1, ")
            output.write(track["title"])
            output.write("\n")
          if path_subst:
            track_file = re.sub(path_subst[0], path_subst[1], track_file)
          output.write(track_file)
          if extm3u:
            output.write("\n\n")
          else:
            output.write("\n")



if __name__ == '__main__':
  with open(expanduser("~/.podcastrc")) as pref_file:
    prefs = json.load(pref_file)
  # print(prefs["db"])
  db = Database(prefs["db"])

  parser = ArgumentParser("Manage and download podcasts")
  subparsers = parser.add_subparsers()
  
  parser_add = subparsers.add_parser("add", help = "Add a podcast to the database")
  parser_add.add_argument('url', type=str, help = "The URL of the podcast")
  parser_add.set_defaults(func = lambda args: db.add(args.url))

  parser_add = subparsers.add_parser("delete", help = "Remove a podcast from the database")
  parser_add.add_argument('id', type=str, help = "The ID of the podcast (see list)")
  parser_add.set_defaults(func = lambda args: db.delete(args.id))

  parser_list = subparsers.add_parser("list", help = "List podcasts")
  parser_list.set_defaults(func = lambda args: print("\n".join( cast.describe() for cast in db.list() )) )

  parser_refresh = subparsers.add_parser("refresh", help = "Update with recent casts")
  parser_refresh.add_argument("--cast", type = int, default = None, help = "The ID of a specific podcast to refresh")
  parser_refresh.set_defaults(func = lambda args: db.refresh(id = args.cast))

  parser_download = subparsers.add_parser("download", help = "Download unavailable casts")
  parser_download.add_argument("directory", type = str, help = "The directory to download to")
  parser_download.add_argument("--refresh", action = 'store_true', help = "Refresh the podcast first")
  parser_download.add_argument("--metadata", action = 'store_true', help = "Overwrite the MP3 metadata with feed values")
  parser_download.add_argument("--cast", type = int, default = None, help = "The ID of a specific podcast to download")
  parser_download.set_defaults(func = lambda args: db.download(args.directory, id = args.cast, refresh_first = args.refresh, update_metadata = args.metadata))

  parser_update_meta = subparsers.add_parser("update-metadata", help = "Update metadata for all downloaded podcasts") 
  parser_update_meta.set_defaults(func = lambda args: db.update_metadata())

  parser_playlists = subparsers.add_parser("gen-playlist", help = "Generate M3U playlists for downloaded podcasts") 
  parser_playlists.add_argument("directory", type = str, help = "The directory to generate playlists in")
  parser_playlists.add_argument("--refresh", action = 'store_true', help = "Refresh the podcast first")
  parser_playlists.add_argument("--download", type = str, help = "Download unavailable casts first to the specified directory")
  parser_playlists.add_argument("--metadata", action = 'store_true', help = "If downloading, overwrite the MP3 metadata with feed values")
  parser_playlists.add_argument("--cast", type = int, default = None, help = "The ID of a specific podcast to download")
  parser_playlists.add_argument("--path-subst", type = str, default = None, help = "Apply a pattern substitution to the file path (pattern^replacement)")
  parser_playlists.set_defaults(func = lambda args: db.generate_playlists(
                                                          args.directory,
                                                          id = args.cast, 
                                                          refresh_first = args.refresh,
                                                          download_first = args.download,
                                                          update_metadata = args.metadata,
                                                          path_subst = args.path_subst))

  args = parser.parse_args()
  args.func(args)
