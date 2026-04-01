# skylerkaufman.com

Barebones static welcome page, ready for [Vercel](https://vercel.com).

## Deploy on Vercel

1. Push this folder to a new GitHub (or GitLab / Bitbucket) repository.
2. In the Vercel dashboard: **Add New… → Project**, import the repo.
3. Leave defaults (framework: Other, output: static root). Deploy.
4. In the project **Settings → Domains**, add `skylerkaufman.com` and `www.skylerkaufman.com`, then follow Vercel’s DNS instructions at your registrar.

### CLI (optional)

```bash
npm i -g vercel
cd skylerkaufman-com
vercel
```

Follow prompts; use `vercel --prod` for production.
