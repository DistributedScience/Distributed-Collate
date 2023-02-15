#
#                                 - [ BROAD'16 ] -
#
# A docker instance for accessing AWS resources
# This wraps your docker registry
#


FROM cellprofiler/pycytominer:latest

# Install S3FS
USER root
RUN apt-get -y update           && \
    apt-get -y upgrade          && \
    apt-get -y install 		\
	automake 		\
	autotools-dev 		\
	g++ 			\
	git 			\
	libcurl4-gnutls-dev 	\
	libfuse-dev 		\
	libssl-dev 		\
	libxml2-dev 		\
	make pkg-config		\
	sysstat			\
	curl

WORKDIR /usr/local/src
RUN git clone https://github.com/s3fs-fuse/s3fs-fuse.git
WORKDIR /usr/local/src/s3fs-fuse
RUN ./autogen.sh
RUN ./configure
RUN make
RUN make install

RUN pip install awscli boto3 watchtower

# SETUP NEW ENTRYPOINT

WORKDIR /workspace/software/pycytominer
COPY collate-worker.py .
COPY instance-monitor.py .
COPY run-worker.sh .
RUN chmod 755 run-worker.sh

WORKDIR /workspace/software/pycytominer
ENTRYPOINT ["./run-worker.sh"]
CMD [""]
