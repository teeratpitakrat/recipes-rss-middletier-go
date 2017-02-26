FROM scratch

COPY middletier /
EXPOSE 9191
CMD ["/middletier"]
