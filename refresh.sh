#!/bin/bash
cd $(dirname $0)
python3 Podcast.py gen-playlist /var/lib/mpd/playlists --path-subst /mnt/Drobo/Music/^ --refresh --download /mnt/Drobo/Music/Playlists --metadata
