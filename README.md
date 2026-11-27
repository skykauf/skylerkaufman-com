# skylerkaufman.com

Apex hub that routes to project subdomains:

- [volleyball.skylerkaufman.com](https://volleyball.skylerkaufman.com/)
- [health.skylerkaufman.com](https://health.skylerkaufman.com/)
- [travel.skylerkaufman.com](https://travel.skylerkaufman.com/)

Deployed as Vercel project **skylerkaufman-hub** (`www` + apex → www).

Legacy volleyball paths (`/volley-chat/`, `/fivb-explorer/`, `/math-meteor/`, `/api/*`, etc.) permanently redirect to the volleyball subdomain via `vercel.json`.

Volleyball app source lives in [`skykauf/volleyball`](https://github.com/skykauf/volleyball).
