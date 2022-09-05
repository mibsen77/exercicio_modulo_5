from asyncio import threads
import csv
from email import header
import random
import concurrent.futures
import requests
from wsgiref import headers
import time

from bs4 import BeautifulSoup


headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
MAX_THREADS=10

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0,0.2))
    response=BeautifulSoup(requests.get(movie_link,headers=headers).content,'html.parser')
    movie_soup=response

    if movie_soup is not None:
        title=None
        date=None

        movie_data=movie_soup.find('div',attrs={'class':'sc-80d4314-1'})
        if movie_data is not None:
            title=movie_data.find('h1').get_text()
            date=movie_data.find('a',attrs={'class':'ipc-link'}).get_text().strip()
        
        rating=movie_soup.find('div',attrs={'data-testid':'hero-rating-bar__aggregate-rating__score'}).get_text() if movie_soup.find('span',attrs={'class':'sc-7ab21ed2-1'}) else None
        plot_text=movie_soup.find('span',attrs={'data-testid':'plot-xs_to_m'}).get_text() if movie_soup.find('div',attrs={'class':'sc-2a827f80-4'}) else None

        with open('movies.csv',mode='a') as file:
            movie_writer=csv.writer(file,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL)
            if all([title,date,rating,plot_text]):
                print(title,date,rating,plot_text)
                movie_writer.writerow([title,date,rating,plot_text])



def extract_movies(soup):
    movies_table=soup.find('table',attrs={'data-caller-name':'chart-moviemeter'}).find('tbody')
    movies_table_rows=movies_table.find_all("tr")
    movie_links=['https://imdb.com'+movie.find('a')['href'] for movie in movies_table_rows]

    threads=min(MAX_THREADS,len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details,movie_links)


def main():
    start_time=time.time()
   
    popular_movies_url='https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response=requests.get(popular_movies_url,headers=headers)
    soup = BeautifulSoup(response.content,'html.parser')

    extract_movies(soup)
    end_time=time.time()
    print('Total time taken:',end_time-start_time)

""" if '__name__'=='__main__':
    print ('main') """

main()
