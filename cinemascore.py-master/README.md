# cinemascore.py
A tiny Python library for getting Cinemascore movie grades. Returns a dict of movie titles and their grades.

Usage:
```
>>> import cinemascore
>>> x = cinemascore.search("star wars")
>>> x
{'STAR WARS: THE LAST JEDI (2017)': 'A', 'ROGUE ONE: A STAR WARS STORY (2016)': 'A', 'STAR WARS: EPISODE VII - THE FORCE AWAKENS (2015)': 'A', 'STAR WARS: THE CLONE WARS (2008)': 'B-', 'STAR WARS EPISODE III: REVENGE OF THE SITH (2005)': 'A-', 'STAR WARS EPISODE 2-ATTACK OF THE CLONES (2002)': 'A-', 'STAR WARS EPISODE 1-THE PHANTOM MENACE (1999)': 'A-'}
```
