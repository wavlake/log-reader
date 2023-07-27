FROM public.ecr.aws/lambda/nodejs:18

# Copy function code and install dependencies
RUN cd ${LAMBDA_TASK_ROOT}
COPY index.js ${LAMBDA_TASK_ROOT}
COPY db.js ${LAMBDA_TASK_ROOT}
COPY package.json ${LAMBDA_TASK_ROOT}
COPY package-lock.json ${LAMBDA_TASK_ROOT}
COPY .env.prod ${LAMBDA_TASK_ROOT}/.env
RUN npm install --omit=dev

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "index.handler" ]