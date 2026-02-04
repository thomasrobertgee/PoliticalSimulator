import axios from 'axios';

// Base API URL - assumes proxy or CORS set up
const API_URL = 'http://localhost:8000/api/v1';

export const fetchStateStats = async (year: number) => {
    // Mock for now if backend not ready, or actual call
    try {
        const res = await axios.get(`${API_URL}/stats/${year}`);
        return res.data;
    } catch (err) {
        console.warn("API Error, using mock data", err);
        return mockData(year);
    }
};

const mockData = (year: number) => ({
    gdp: 400 + (year - 2000) * 10,
    population: 6.5 + (year - 2000) * 0.1,
    debt: 50 + (year - 2010) * 5,
    gini: 0.3 + (year - 2000) * 0.001
});
