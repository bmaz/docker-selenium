FROM python:3.5

# Install Firefox
RUN \
    curl 'https://download-installer.cdn.mozilla.net/pub/firefox/releases/35.0/linux-x86_64/en-US/firefox-35.0.tar.bz2' \
        -o firefox.tar.bz2 &&\
    bunzip2 firefox.tar.bz2 &&\
    tar xf firefox.tar &&\
    rm firefox.tar

RUN apt-get update && apt-get install -y \
    # Headless browser support
    xvfb \
    # Needed to launch firefox
    libasound2 \
    libgtk2.0-0 \
    libdbus-glib-1-2 \
    libxcomposite1

RUN pip install requests==2.5.1 selenium==2.52.0 pyvirtualdisplay==0.2.1 beautifulsoup4==4.5.1 lxml==3.6.4

ENV PYTHONPATH /

ENTRYPOINT [ "/entrypoint.sh" ]

CMD [ "python", "-u", "/main.py" ]

COPY root /