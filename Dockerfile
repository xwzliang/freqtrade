# FROM freqtradeorg/freqtrade:develop_plot

# USER root
# RUN rm -rf /freqtrade/

# USER ftuser
# # Install and execute
# COPY --chown=ftuser:ftuser . /freqtrade/

# RUN pip install -e . --user --no-cache-dir \
#   && mkdir /freqtrade/user_data/ \
#   && freqtrade install-ui

# RUN pip install --user --no-cache-dir tushare holidays

FROM xwzliang/freqtrade:0.0.1

USER root
RUN rm -rf /freqtrade/
RUN rm -rf /freqtrade/
USER ftuser
# Install and execute
COPY --chown=ftuser:ftuser . /freqtrade/

ARG FREQTRADE_UI_REPO=https://api.github.com/repos/xwzliang/frequi
ENV FREQTRADE_UI_REPO=$FREQTRADE_UI_REPO

RUN pip install -e . --user --no-cache-dir \
  && mkdir /freqtrade/user_data/ \
  && freqtrade install-ui