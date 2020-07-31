import os
import pandas as pd

from sys import argv, exit
from tqdm import tqdm
from bs4 import BeautifulSoup


"""
    From the website: https://www.wordproject.org/
"""

if len(argv) != 3:
    print("Usage: scrapper_v2.py directory language")
    exit(1)

BOOKS = ['GEN','EXO','LEV','NUM','DEU',
         'JOS','JDG','RUT','1SA','2SA',
         '1KI','2KI','1CH','2CH','EZR',
         'NEH','EST','JOB','PSA','PRO',
         'ECC','SNG','ISA','JER','LAM',
         'EZK','DAN','HOS','JOL','AMO',
         'OBA','JON','MIC','NAM','HAB',
         'ZEP','HAG','ZEC','MAL','MAT',
         'MRK','LUK','JHN','ACT','ROM',
         '1CO','2CO','GAL','EPH','PHP',
         'COL','1TH','2TH','1TI','2TI',
         'TIT','PHM','HEB','JAS','1PE',
         '2PE','1JN','2JN','3JN','JUD',
         'REV']

BIBLE_DIR = argv[1]
LANGUAGE = argv[2]

def main():
    # Run
    df = parse_htms(BIBLE_DIR, LANGUAGE)
    output_file = LANGUAGE + '.csv'
    df.to_csv(output_file, index=False)

def parse_htms(directory, lang):
    """
        Parses the entire directory and returns a DataFrame of the verses.
    """
    paths  = []
    df = pd.DataFrame(columns=['id', lang])

    for dirname, _, filenames in os.walk(directory):
        for filename in filenames:
            book_number = dirname.split('/')[-1]
            try:
                book_number = int(book_number)
            except ValueError:
                continue
            if not filename.endswith('.htm'):
                continue
            pairs = (book_number, os.path.join(dirname, filename))
            paths.append(pairs)

    for book_number, path in tqdm(paths):
        # Create ID
        book_name = BOOKS[book_number - 1]
        chapter = path.split('/')[-1].split('.')[0]
        id = book_name + str(chapter)

        soup = BeautifulSoup(open(path), 'html.parser', from_encoding='utf-8')
        verses = soup.select('.textBody p')[0].get_text().strip()
        verses = verses.replace('1 \n', '1 ')
        verses = verses.split('\n')

        for verse in verses:
            content = verse.split(' ')
            verse_number = content[0]
            id_ = id + verse_number
            verse_content = ' '.join(content[1:])
            temp = pd.DataFrame([[id, verse_content]],
                                columns=['id', lang])
            df = df.append(temp, ignore_index=False)

    return df


if __name__ == '__main__':
    main()
