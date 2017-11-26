
from django.conf.urls import url , include
from django.contrib import admin
admin.autodiscover()
from trucking.views import login_response

urlpatterns = [
    url(r"^admin/", admin.site.urls),
    url(r'^o/', include('oauth2_provider.urls', namespace='oauth2_provider')),
    url(r'^$', login_response.index, name='index'),

]