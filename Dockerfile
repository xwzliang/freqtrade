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

USER ftuser
# Install and execute
COPY --chown=ftuser:ftuser . /freqtrade/

RUN pip install -e . --user --no-cache-dir \
  && mkdir /freqtrade/user_data/ \
  && freqtrade install-ui