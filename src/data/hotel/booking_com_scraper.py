import logging
import extruct
import requests
from w3lib.html import get_base_url
import re

def scrap_booking_com():
    logger = logging.getLogger()

    # hotel_urls_frankfurt = ['https://www.booking.com/hotel/de/rocco-forte-villa-kennedy.html',
    #               'https://www.booking.com/hotel/de/toyoko-inn-frankfurt-am-main-hauptbahnhof.html',
    #               'https://www.booking.com/hotel/de/nhfrankfurtcity.html',
    #               'https://www.booking.com/hotel/de/flemming-s-deluxe-frankfurt-city.html',
    #               'https://www.booking.com/hotel/de/niu-charly.html',
    #               'https://www.booking.com/hotel/de/ichfrankf.html',
    #               'https://www.booking.com/hotel/de/leonardo-hotel-frankfurt-city-center.html',
    #               'https://www.booking.com/hotel/de/excelsiorfrankfurtmain.html',
    #               'https://www.booking.com/hotel/de/moxy-frankfurt-city-center.html',
    #               'https://www.booking.com/hotel/de/intercityhotel-frankfurt-hauptbahnhof.html',
    #               'https://www.booking.com/hotel/de/bristolhotelff.html',
    #               'https://www.booking.com/hotel/de/leonardo-royal-frankfurt.html',
    #               'https://www.booking.com/hotel/de/hilton-garden-inn-frankfurt-city-centre.html',
    #               'https://www.booking.com/hotel/de/maritimhotelfrankfurt_frankfurt.html',
    #               'https://www.booking.com/hotel/de/ameron-frankfurt-neckarvillen-boutique.html',
    #               'https://www.booking.com/hotel/de/pearl.html',
    #               'https://www.booking.com/hotel/de/innside-premium-suites-frankfurt-eurotheum.html',
    #               'https://www.booking.com/hotel/de/frankfurt-marriott.html',
    #               'https://www.booking.com/hotel/de/lindnerhotelresidencemainplaza.html',
    #               'https://www.booking.com/hotel/de/sofitel-frankfurt-opera.html'
    #               ]

    hotel_urls_new_york = [
        'https://www.booking.com/hotel/us/the-park-ave-north-new-york.html',
        'https://www.booking.com/hotel/us/50-bowery.html',
        'https://www.booking.com/hotel/us/the-manhattan-club.html',
        'https://www.booking.com/hotel/us/be.html',
        'https://www.booking.com/hotel/us/new-york-midtown-manhattan-fifth-avenue.html',
        'https://www.booking.com/hotel/us/the-bed-amp-breakfast-on-central-park.html',
        'https://www.booking.com/hotel/us/solita-soho-new-york.html',
        'https://www.booking.com/hotel/us/41-at-times-square.html',
        'https://www.booking.com/hotel/us/wingate-by-wyndham-manhattan-chelsea.html',
        'https://www.booking.com/hotel/us/the-paul-an-ascend-collection-member-new-york.html',
        'https://www.booking.com/hotel/us/hotel-lower-east-side-new-york.html',
        'https://www.booking.com/hotel/us/leo-house.html',
        'https://www.booking.com/hotel/us/31.html',
        'https://www.booking.com/hotel/us/hampton-inn-msga.html',
        'https://www.booking.com/hotel/us/hilton-garden-inn-new-york-chelsea.html',
        'https://www.booking.com/hotel/us/fairfield-inn-by-marriott-new-york-manhattan-financial-district.html',
        'https://www.booking.com/hotel/us/four-points-by-sheraton-manhattan-chelsea.html',
        'https://www.booking.com/hotel/us/la-quinta-new-york-city-central-park.html',
        'https://www.booking.com/hotel/us/hampton-inn-seaport-financial-district.html',
        'https://www.booking.com/hotel/us/heritage-new-york-city.html'
    ]

    hotel_urls_tokio = [
        'https://www.booking.com/hotel/jp/centurion-grand-akasaka.en-gb.html',
        'https://www.booking.com/hotel/jp/itosuhui-bi-shou.en-gb.html',
        'https://www.booking.com/hotel/jp/hoteruribumatukusudong-jing-li-chuan-yi-qian.en-gb.html',
        'https://www.booking.com/hotel/jp/imano-tokyo-ginza-hostel.en-gb.html',
        'https://www.booking.com/hotel/jp/uhomeshang-ye-hoteru.en-gb.html',
        'https://www.booking.com/hotel/jp/time-sharing-stay-asakusa-vacation-stay-35324v.en-gb.html',
        'https://www.booking.com/hotel/jp/hermitage-ochiai-vacation-stay-48795.en-gb.html',
        'https://www.booking.com/hotel/jp/artist-bna-studio-akihabara.en-gb.html',
        'https://www.booking.com/hotel/jp/grids-tokyo-ueno-amp-hostel.en-gb.html',
        'https://www.booking.com/hotel/jp/comfort-inn-tokyo-roppongi.en-gb.html',
        'https://www.booking.com/hotel/jp/sotetsu-fresa-inn-tokyokyobashi.en-gb.html',
        'https://www.booking.com/hotel/jp/comfort-tokyo-higashi-kanda.en-gb.html',
        'https://www.booking.com/hotel/jp/nestle-tokyo-cozy-hostel.en-gb.html',
        'https://www.booking.com/hotel/jp/jal-city-haneda-tokyo.en-gb.html',
        'https://www.booking.com/hotel/jp/apa-shibuya-dogenzaka.en-gb.html',
        'https://www.booking.com/hotel/jp/soutetsu-fresa-inn-tokyo-roppongi.en-gb.html',
        'https://www.booking.com/hotel/jp/intergate-tokyo-kyobashi.en-gb.html',
        'https://www.booking.com/hotel/jp/chisun-grand-akasaka.en-gb.html',
        'https://www.booking.com/hotel/jp/best-western-hotel-fino-tokyo.en-gb.html',
        'https://www.booking.com/hotel/jp/apa-shinbashi-toranomon.en-gb.html'
    ]

    product_urls = ['https://www.mindfactory.de/product_info.php/Razer-Kiyo-Streaming-Webcam-mit-Beleuchtungsring-schwarz_1234049.html']
    movie_urls = [
        'https://www.imdb.com/title/tt0480255/',
        'https://www.imdb.com/title/tt1375666/',
        'https://www.imdb.com/title/tt0446029/',
        'https://www.imdb.com/title/tt1130884/',
        'https://www.imdb.com/title/tt0938283/',
        'https://www.imdb.com/title/tt1325004/',
        'https://www.imdb.com/title/tt1375670/',
        'https://www.imdb.com/title/tt0840361/',
        'https://www.imdb.com/title/tt0926084/',
        'https://www.imdb.com/title/tt1228705/',
        'https://www.imdb.com/title/tt1285016/',
        'https://www.imdb.com/title/tt0398286/',
        'https://www.imdb.com/title/tt1231587/',
        'https://www.imdb.com/title/tt0817177/',
        'https://www.imdb.com/title/tt1014759/',
        'https://www.imdb.com/title/tt0955308/',
        'https://www.imdb.com/title/tt0435761/',
        'https://www.imdb.com/title/tt1250777/',
        'https://www.imdb.com/title/tt1181791/',
        'https://www.imdb.com/title/tt0815236/',
        'https://www.imdb.com/title/tt1320253/',
        'https://www.imdb.com/title/tt1440728/',
        'https://www.imdb.com/title/tt1323594/',
        'https://www.imdb.com/title/tt1020558/',
        'https://www.imdb.com/title/tt1038686/',
        'https://www.imdb.com/title/tt1403981/'
        'https://www.imdb.com/title/tt1126591/',
        'https://www.imdb.com/title/tt1542344/'
        # 'https://www.imdb.com/title/tt1512235/',
        # 'https://www.imdb.com/title/tt0947798/',
        # 'https://www.imdb.com/title/tt1458175/',
        # 'https://www.imdb.com/title/tt1255953/',
        # 'https://www.imdb.com/title/tt0758752/',
        # 'https://www.imdb.com/title/tt1591095/',
        # 'https://www.imdb.com/title/tt1120985/'
        # 'https://www.imdb.com/title/tt1242432/',
        # 'https://www.imdb.com/title/tt1273235/',
    ]

    entity_id = 0

    for movie_url in movie_urls:
        r = requests.get(movie_url)
        base_url = get_base_url(r.text, r.url)
        data = extruct.extract(r.text, base_url=base_url)
        #print(data)

        movie = {}
        movie['entityId'] = entity_id
        movie['name'] = data['json-ld'][0]['name']
        movie['director.name'] = data['json-ld'][0]['director'][0]['name']
        movie['duration'] = data['json-ld'][0]['duration']
        movie['datePublished'] = data['json-ld'][0]['datePublished']


        # hotel['name'] = data['json-ld'][0]['name']
        # hotel['address.addressregion'] = data['json-ld'][0]['address']['addressRegion']
        # hotel['address.addresslocality'] = data['json-ld'][0]['address']['addressLocality']
        # hotel['address.addresscountry'] = data['json-ld'][0]['address']['addressCountry']
        # hotel['address.postalcode'] = data['json-ld'][0]['address']['postalCode']
        # hotel['address.streetaddress'] = data['json-ld'][0]['address']['streetAddress']
        # #hotel['telephone'] = data['json-ld'][0]['telephone']
        #
        # temp_locality = hotel['address.streetaddress'].split(hotel['address.postalcode'])[0].replace(hotel['address.addresslocality'],'')
        # temp_locality = re.sub('^,\\s', '', temp_locality)
        # temp_locality = re.sub(',\\s$', '', temp_locality)
        # hotel['address.streetaddress'] = hotel['address.addresslocality']
        # hotel['address.addresslocality'] = temp_locality

        #for addressPart in hotel['streetAddress'].split(','):
        #    if hotel['postalCode'] in addressPart:
        #        addressPart = addressPart.replace(hotel['postalCode'], '')
        #        hotel['city'] = addressPart.strip()

        #del hotel['address.addresscountry']
        #del hotel['address.streetaddress']
        #del hotel['address.addresslocality']
        #del hotel['address.addressregion']
        print(str(movie) + ',')

        entity_id += 1

        #print(data['json-ld'][0]['address'])
    logger.info('Loaded json ld data')

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    scrap_booking_com()