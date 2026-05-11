import pandas as pd
import numpy as np
import plotly.express as px
import os

class BeamDataPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.raw_data = None
        self.cleaned_data = None
        
    def ingest_data(self):
        """Module 1: Robust Data Ingestion"""
        try:
            self.raw_data = pd.read_csv(self.file_path)
            print("✅ Data ingested successfully.")
        except FileNotFoundError:
            print("❌ Error: CSV file not found. Check your data/ folder.")
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")

    def clean_and_filter(self):
        """Module 2 & 3: Data Cleaning and Unique Filtering"""
        if self.raw_data is not None:
            # Automated cleaning
            df = self.raw_data.copy()
            df = df.dropna()
            df = df.drop_duplicates()
            
            # Unique Filter: Isolating beams with height > 450 mm
            self.cleaned_data = df[df['Beam_Height_mm'] > 450].copy()
            
            # Ensure directories exist
            os.makedirs('data', exist_ok=True)
            self.cleaned_data.to_csv('data/dataset_cleaned.csv', index=False)
            print(f"✅ Data cleaned and filtered. Shape: {self.cleaned_data.shape}")

    def run_statistics(self):
        """Module 4: Engineering Data Analytics using NumPy"""
        if self.cleaned_data is not None:
            # Mandatory use of NumPy
            strength = self.cleaned_data['Bearing_Capacity_kN'].values 
            
            metrics = {
                "Mean": np.mean(strength),
                "Median": np.median(strength),
                "Std Dev": np.std(strength),
                "Variance": np.var(strength)
            }
            
            print("\n--- Engineering Metrics ---")
            for key, val in metrics.items():
                print(f"{key}: {val:.2f}")
            return metrics

    def generate_visuals(self):
        """Module 5: Visualization & Animation"""
        if self.cleaned_data is not None:
            os.makedirs('outputs', exist_ok=True) 
            
            # Static Plot: Histogram
            fig1 = px.histogram(self.cleaned_data, x="Bearing_Capacity_kN", title="Beam Capacity Distribution")
            fig1.write_image("outputs/capacity_histogram.png")
            
            # Animated Plot: Capacity vs Reinforcement Ratio over Concrete Grade
            # We sort by grade first so the animation plays in order
            df_anim = self.cleaned_data.sort_values(by="Concrete_Grade_MPa") 
            fig2 = px.scatter(df_anim, x="Longitudinal_Reinforcement_Ratio_percent", y="Bearing_Capacity_kN", 
                              animation_frame="Concrete_Grade_MPa", 
                              title="Beam Capacity Dynamics by Concrete Grade")
            fig2.write_html("outputs/capacity_animation.html")
            print("✅ Visuals saved to outputs/ folder.")

# Execution
if __name__ == "__main__":
    pipeline = BeamDataPipeline('data/rc_beam_bearing_capacity_dataset.csv')
    pipeline.ingest_data()
    pipeline.clean_and_filter()
    pipeline.run_statistics()
    pipeline.generate_visuals()