FROM centos

USER ROOT

COPY requirements.txt /tmp/
RUN yum install -y python epel-release
RUN yum install -y python-pip
RUN pip install --requirement /tmp/requirements.txt

USER 1001
