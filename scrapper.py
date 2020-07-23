# scrapper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from tqdm import tqdm

"""
    Notes:
        This script scraps the website, bible.com.
"""

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

ENGLISH_VERSION = 'KJV'
BASE_URL_ENGLISH= r'https://www.bible.com/bible/1/'

LUGANDA_VERSION = 'EEEE'
BASE_URL_LUGANDA = r'https://www.bible.com/bible/1625/'

def main():
    # 1. Create directories
    for book in BOOKS:
        make_dirs('ENG', book)          # English
        make_dirs('LUG', book)          # Luganda

    # 2. Web scrapping
    #    English
    scrapping(BOOKS, 'ENG', ENGLISH_VERSION, BASE_URL_ENGLISH)
    #    Luganda
    scrapping(BOOKS, 'LUG', LUGANDA_VERSION, BASE_URL_LUGANDA)

    # Should run like magic.


class Scrapper:
    def __init__(self, chapter_label, chapter_content):
        self.chapter_label = chapter_label
        self.chapter_content = chapter_content

    def scrap(self):
        """
            This function obtains the page from at the URL, and scraps it.
        """
        paragraphs = self.chapter_content.find_all('div', class_='p')

        # Get all the verses.
        df = pd.DataFrame(columns=['chapter', 'verse', 'content'])
        for paragraph in paragraphs:
            verse_object = paragraph.select('span.label')
            verse = int(verse_object[0].get_text())

            verse_html = paragraph.select('.verse .content')
            verse_holder = ''
            for content in verse_html:
                temp_ = content.get_text()
                if temp_.isspace():
                    continue
                else:
                    verse_holder += temp_

            temp = pd.DataFrame([[self.chapter_label, verse, verse_holder]],
                                columns=['chapter', 'verse', 'content'])
            df = df.append(temp, ignore_index=True)
        return df

class LugScrapper(Scrapper):
    """
        Extends the Scrapper class to scrap Luganda.
    """
    def __init__(self, chapter_label, chapter_content):
        """
            This is essentially the same as the Scrapper.__init__().
        """
        super().__init__(chapter_label, chapter_content)

    def scrap(self):
        """
            This function scraps the bible.com/EEEE for Luganda.
        """
        paragraphs = self.chapter_content.find_all('div', class_='p')

        # Get all the verses.
        df = pd.DataFrame(columns=['chapter', 'verse', 'content'])
        for paragraph in paragraphs:
            dump = self._parse_paragraph(paragraph)
            for verse, content in dump:
                temp = pd.DataFrame([[self.chapter_label, verse, content]],
                                columns=['chapter', 'verse', 'content'])
                df = df.append(temp, ignore_index=True)
        return df

    def _parse_paragraph(self, paragraph):
        """
            Extracts the verse number and the content from an HTML object.
        """
        verse_html = paragraph.select('.verse .label')
        verses = []
        for label in verse_html:
            verse = None
            try:
                verse = int(label.get_text())
            except ValueError:
                continue
            verses.append(verse)

        verse_content = []
        content_html = paragraph.select('.verse .content')
        for content in content_html:
            c_ = None
            temp = content.get_text()
            if temp.isspace():
                continue
            else:
                c_ = temp.strip()
                verse_content.append(c_)
        return zip(verses, verse_content)


class UrlContentIterator:
    """
        Automate the iteration from Genesis Chapter 1, to Revelation 22.
    """

    def __init__(self, book, bible_version, base_url):
        self.book = book
        self.tracker = 0                # First chapter is one (1).
        # self.last_tracker = 0
        self.version = bible_version
        self.base_url = base_url

    def __iter__(self):
        """
            An iterator returns itself.
        """
        return self

    def url(self):
        """
            Return the current URL.
        """
        return os.path.join(self.base_url, f'{self.book}.{self.tracker + 1}.{self.version}')

    def __next__(self):
        """
            Returns the content from the next URL in the sequence.
        """
        # Advance and update to chapter
        self.tracker += 1

        URL = os.path.join(self.base_url, f'{self.book}.{self.tracker}.{self.version}')
        request = requests.get(URL)

        if request.status_code != 200:
            raise RuntimeError(f"Content at {URL} couldn't be retrieved.")

        if not request.history:
            content = BeautifulSoup(request.content, 'html.parser')
            chapter = content.find('div', class_="chapter")
            chapter_label = int(content.find('div', class_='label').get_text())
            return (chapter_label, chapter)
        else:
            raise StopIteration()


def make_dirs(lang, dir):
    """
        Create directory if it doesn't already exist.
    """
    cur_dir = os.getcwd()
    path = os.path.join(cur_dir, lang, dir)
    if not os.path.isdir(path):
        os.makedirs(path)

def scrapping(books, lang, version, base_url):
    """
        Runs the scrapping process.
    """
    for book in tqdm(books, desc=f'{lang} Scrapping: '):
        path = os.path.join(os.getcwd(), lang, book)
        dump = UrlContentIterator(book, version, base_url)
        for content in dump:
            scrap = None
            if lang == 'LUG':
                scrap = LugScrapper(content[0], content[1])
            else:
                scrap = Scrapper(content[0], content[1])
            scrap_ = scrap.scrap()
            scrap_.to_csv(os.path.join(path, f'{content[0]}.csv'), index=False)


if __name__ == '__main__':
    main()
