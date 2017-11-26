from django.shortcuts import render
import logging
from django.http import HttpResponse
import datetime
from trucking.databases.model import dao
from oauth2_provider.decorators import protected_resource

logger = logging.getLogger(__name__)


@protected_resource()
def index(request):
    db = dao('eld_table', 'id')
    reqs = {'eld_registration_id': '1234',
            'eld_identifier':'5678',
            'eld_auth_value':'val'
            }
    res = db.create([reqs])
    print(res)
    return HttpResponse("Hello")



    # logger.debug('Something went wrong!')
    # print("Hello world")
    # now = datetime.datetime.now()
    # html = "<html><body>It is now %s.</body></html>" % now
    # i = 10
    # if i == 10:
    #     logger.error("error occured")
    #
    #     return HttpResponse(html)


