import type { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.basenerd.app',
  appName: 'Basenerd',
  webDir: 'www',

  server: {
    // Points the WebView at your live Render deployment
    url: 'https://basenerd-backend.onrender.com',
    cleartext: false,
  },

  ios: {
    // Allow navigation within your domain + MLB API calls
    allowsLinkPreview: false,
    scrollEnabled: true,
    contentInset: 'always',
    scheme: 'basenerd',
  },

  plugins: {
    SplashScreen: {
      launchAutoHide: true,
      launchShowDuration: 1500,
      backgroundColor: '#0b0e13',
      showSpinner: false,
    },
  },
};

export default config;
