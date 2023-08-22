"""examanalytics URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from examelk import views

urlpatterns = [
    path('delete-collections/', views.delete_collections, name='delete_collections'),
    path('kvm/', views.kvm, name='kvm'),
    path('create-slot/', views.create_slot, name='create-slot'),
    path('admin/', admin.site.urls),
    path('',views.login.as_view(),name='login'),
    path('manual/',views.Manual.as_view(),name='manual'),
    path('examscript/',views.ExamScript.as_view(),name='examscript'),
    path('config/',views.HomeTemplate.as_view(),name='config'),
    path('edit/', views.Edit.as_view(), name='edit'),
    path('errors/',views.ErrorView.as_view(),name='errors'),
    path('logout/', views.Logout.as_view(), name='logout'),
]
