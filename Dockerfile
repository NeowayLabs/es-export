FROM scratch

MAINTAINER Guilherme Santos <guilherme.santos@neoway.com.br>

ADD ./es-export /opt/es-export/bin/

WORKDIR /opt/es-export/bin

ENTRYPOINT ["./es-export"] 
