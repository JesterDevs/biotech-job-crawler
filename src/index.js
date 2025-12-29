import express from "express";

const app = express();
const PORT = process.env.PORT || 3000;

// Health check (Railway requires something listening)
app.get("/", (req, res) => {
  res.send("Job crawler is running");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
