/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                'cyber-slate': '#0f172a',
                'neon-teal': '#2dd4bf',
                'stress-amber': '#f59e0b',
                'void-black': '#020617',
                'glass-panel': 'rgba(15, 23, 42, 0.7)',
            },
            fontFamily: {
                mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', "Liberation Mono", "Courier New", 'monospace'],
            }
        },
    },
    plugins: [],
}
