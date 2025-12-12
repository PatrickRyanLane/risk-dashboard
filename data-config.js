// Configuration for data source
// When running on Cloud Run, fetch data from GitHub Pages
// When running locally, use relative paths

const DATA_SOURCE = {
  // Set this to your GitHub Pages URL
  GITHUB_PAGES: 'https://patrickryanlane.github.io/news-sentiment-dashboard',
  
  // Set to true to use GitHub Pages, false to use local paths
  USE_GITHUB_PAGES: true
};

// Helper function to get the correct data path
function getDataPath(relativePath) {
  if (DATA_SOURCE.USE_GITHUB_PAGES) {
    return `${DATA_SOURCE.GITHUB_PAGES}/${relativePath}`;
  }
  return relativePath;
}

// Example usage:
// Instead of: fetch('data/stock_prices/2024-01-01-stock-data.csv')
// Use:        fetch(getDataPath('data/stock_prices/2024-01-01-stock-data.csv'))
